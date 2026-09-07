use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use tycho_core::s3::S3Client;
use tycho_core::storage::{
    CoreStorage, PersistentState, PersistentStateKind, PersistentStateMeta, PersistentStatePrefix,
    validate_persistent_state_split_metadata,
};
use tycho_util::metrics::HistogramGuard;

use crate::config::UploaderConfig;
use crate::subscribers::helpers::retry;

mod state_upload_plan;

use state_upload_plan::{StateObjectRole, StateUploadPlan, UploadAction};

pub struct StateUploader {
    inner: Arc<Inner>,
}

impl StateUploader {
    pub fn new(
        config: UploaderConfig,
        storage: CoreStorage,
        s3_client: S3Client,
    ) -> anyhow::Result<Self> {
        let inner = Arc::new(Inner {
            config,
            storage,
            s3_client,
        });

        Ok(StateUploader { inner })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("state uploader started");

        self.inner.run().await.context("state uploader failed")?;

        tracing::info!("state uploader finished");

        Ok(())
    }
}

struct Inner {
    config: UploaderConfig,
    storage: CoreStorage,
    s3_client: S3Client,
}

impl Inner {
    async fn run(&self) -> anyhow::Result<()> {
        let storage = self.storage.persistent_state_storage();

        let (states, mut rx) = storage.subscribe();

        for state in states {
            self.upload_state(state).await?;
        }

        while let Some(state) = rx.recv().await {
            self.upload_state(state).await?;
        }

        Ok(())
    }

    async fn upload_state(&self, state: PersistentState) -> anyhow::Result<()> {
        let block_id = state.block_id();
        let seqno = block_id.seqno;

        let labels = [("workchain", block_id.shard.workchain().to_string())];

        let histogram = HistogramGuard::begin("tycho_uploader_upload_persistent_state_time");

        tracing::info!("started");
        let guard = scopeguard::guard((), |_| {
            tracing::warn!("cancelled");
        });

        // Upload persistent state to S3
        self.upload_state_impl(state).await?;

        metrics::gauge!("tycho_uploader_last_uploaded_state_seqno", &labels).set(seqno as f64);

        // Done
        scopeguard::ScopeGuard::into_inner(guard);
        tracing::info!(
            elapsed = %humantime::format_duration(histogram.finish()),
            "finished"
        );

        Ok(())
    }

    async fn upload_state_impl(&self, state: PersistentState) -> anyhow::Result<()> {
        let storage = &self.storage;
        let s3_client = self.s3_client.client();

        let block_id = state.block_id();
        let kind = state.kind();
        let state_info = storage
            .persistent_state_storage()
            .get_state_info(block_id, kind)
            .context("persistent state not found")?;

        match kind {
            PersistentStateKind::Shard => {
                validate_persistent_state_split_metadata(
                    block_id.shard,
                    state_info.split_depth,
                    state_info.parts.iter().map(|part| part.prefix),
                )?;
            }
            PersistentStateKind::Queue => {
                anyhow::ensure!(
                    state_info.split_depth == 0,
                    "unexpected split depth for persistent queue"
                );
                anyhow::ensure!(
                    state_info.parts.is_empty(),
                    "unexpected parts for persistent queue"
                );
            }
        }

        let root_prefix = if state_info.split_depth == 0 {
            PersistentStatePrefix::Unsplit
        } else {
            PersistentStatePrefix::Split(None)
        };
        let main_location = self.s3_client.make_state_key(block_id, kind, root_prefix)?;
        let manifest_location = self.s3_client.make_state_meta_key(block_id);
        let meta = (state_info.split_depth > 0).then(|| {
            PersistentStateMeta::new(
                state_info.split_depth,
                state_info.parts.iter().map(|part| part.prefix).collect(),
            )
        });
        let mut parts = Vec::with_capacity(state_info.parts.len());
        if let Some(meta) = &meta {
            for &prefix in &meta.parts {
                let part = state_info
                    .parts
                    .iter()
                    .find(|part| part.prefix == prefix)
                    .context("persistent state part is missing from local metadata")?;
                parts.push((
                    prefix,
                    self.s3_client.make_state_key(
                        block_id,
                        kind,
                        PersistentStatePrefix::Split(Some(prefix)),
                    )?,
                    part.size.get(),
                ));
            }
        }
        let plan = StateUploadPlan::make(
            s3_client.as_ref(),
            kind,
            (&main_location, state_info.size.get()),
            &manifest_location,
            meta.as_ref(),
            &parts,
            &self.config,
        )
        .await?;

        for ((prefix, action), (_, _, size)) in plan.parts.iter().zip(&parts) {
            if *action == UploadAction::Upload {
                self.upload_state_object(
                    &state,
                    StateObjectRole::Part(*prefix),
                    PersistentStatePrefix::Split(Some(*prefix)),
                    *size,
                )
                .await?;
            }
        }

        if let (Some(UploadAction::Upload), Some(meta)) = (plan.manifest, meta.as_ref()) {
            let bytes = meta.to_bytes()?;
            self.upload_manifest(&manifest_location, &bytes).await?;
        }

        if plan.main == UploadAction::Upload {
            self.upload_state_object(
                &state,
                StateObjectRole::Main,
                root_prefix,
                state_info.size.get(),
            )
            .await?;
        }

        Ok(())
    }

    async fn upload_manifest(&self, location: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        let role = StateObjectRole::Manifest;
        let declared_size = bytes.len() as u64;

        let operation = format!("upload state manifest {location}");
        retry(&operation, self.config.retry_delay, || async {
            tracing::info!(%role, declared_size, "starting state upload");
            self.s3_client
                .client()
                .put(
                    location,
                    PutPayload::from_bytes(Bytes::copy_from_slice(bytes)),
                )
                .await
                .context("failed to upload state manifest")?;
            verify_uploaded_manifest_bytes(self.s3_client.client().as_ref(), location, bytes)
                .await
                .context("failed to verify uploaded state manifest")?;
            tracing::info!(%role, declared_size, "state manifest upload completed successfully");
            Ok::<(), anyhow::Error>(())
        })
        .await;
        Ok(())
    }

    async fn upload_state_object(
        &self,
        state: &PersistentState,
        role: StateObjectRole,
        prefix: PersistentStatePrefix,
        total_size: u64,
    ) -> anyhow::Result<()> {
        let storage = &self.storage;
        let s3_client = self.s3_client.client();
        let s3_chunk_size = self.s3_client.chunk_size().get() as usize;
        let block_id = state.block_id();
        let kind = state.kind();
        let location = self.s3_client.make_state_key(block_id, kind, prefix)?;

        // retry until we successfully upload
        let operation = format!("upload state object {role} {location}");
        retry(&operation, self.config.retry_delay, || async {
            tracing::info!(
                %role,
                declared_size = total_size,
                ?block_id,
                ?kind,
                "starting state upload"
            );

            let upload = s3_client
                .put_multipart(&location)
                .await
                .context("failed to initialize multipart upload")?;

            // Buffer for MD5 hashes for all chunks
            let mut md5_buffer = vec![];

            let mut uploaded = 0;
            let mut offset = 0u64;

            let mut writer = WriteMultipart::new_with_chunk_size(upload, s3_chunk_size);

            let mut part_len = 0usize;
            let mut part_hasher = md5::Context::new();

            // Read state in chunks and write to S3
            while offset < total_size {
                // Read chunk from persistent state storage
                let state_chunk = storage
                    .persistent_state_storage()
                    .read_state_chunk(block_id, offset, kind, prefix)
                    .await
                    .with_context(|| format!("failed to read state chunk at offset {offset}"))?;

                // Process the chunk byte by byte, accumulating into S3 parts
                let mut chunk_offset = 0;
                while chunk_offset < state_chunk.len() {
                    // Wait for capacity before starting a new S3 part
                    if part_len == 0 {
                        writer
                            .wait_for_capacity(self.config.max_concurrency)
                            .await
                            .context("failed to acquire upload state capacity")?;
                    }

                    let remaining_in_part = s3_chunk_size - part_len;
                    let remaining_in_chunk = state_chunk.len() - chunk_offset;

                    let to_copy = remaining_in_chunk.min(remaining_in_part);

                    let slice = &state_chunk[chunk_offset..chunk_offset + to_copy];

                    writer.write(slice);
                    part_hasher.consume(slice);

                    part_len += slice.len();
                    chunk_offset += slice.len();

                    // If we filled the S3 part, finalize it
                    if part_len == s3_chunk_size {
                        let digest =
                            std::mem::replace(&mut part_hasher, md5::Context::new()).finalize();
                        md5_buffer.extend_from_slice(digest.0.as_slice());
                        part_len = 0;
                    }
                }

                uploaded += state_chunk.len();

                // Next storage chunk
                offset += state_chunk.len() as u64;
            }

            // Finalize the last partial part if any
            if part_len > 0 {
                let digest = std::mem::replace(&mut part_hasher, md5::Context::new()).finalize();
                md5_buffer.extend_from_slice(digest.0.as_slice());
            }

            let result = writer
                .finish()
                .await
                .context("failed to complete state upload")?;
            let expected_etag = hex::encode(md5::compute(&md5_buffer).as_slice());

            anyhow::ensure!(
                result
                    .e_tag
                    .as_deref()
                    .is_some_and(|tag| tag.trim_matches('"').starts_with(&expected_etag)),
                "state ETag mismatch detected: expected={expected_etag}, received={:?}",
                result.e_tag
            );

            tracing::info!(?block_id, ?kind, %role, declared_size = total_size, uploaded, "upload state object completed successfully");
            Ok::<(), anyhow::Error>(())
        })
        .await;

        Ok(())
    }
}

async fn verify_existing_manifest(
    client: &dyn ObjectStore,
    location: &Path,
    expected_meta: &PersistentStateMeta,
    retry_delay: std::time::Duration,
) -> anyhow::Result<()> {
    let actual_bytes = retry(
        &format!("read remote manifest {location}"),
        retry_delay,
        || async {
            // restart the entire read if fetching the response body fails
            client
                .get(location)
                .await
                .context("failed to read remote manifest")?
                .bytes()
                .await
                .context("failed to read remote manifest bytes")
        },
    )
    .await;
    let actual_meta = PersistentStateMeta::from_bytes(&actual_bytes)
        .context("failed to parse remote manifest")?
        .context("remote manifest is missing")?;
    anyhow::ensure!(
        actual_meta == *expected_meta,
        "remote manifest does not match the local split metadata"
    );
    Ok(())
}

async fn verify_uploaded_manifest_bytes(
    client: &dyn ObjectStore,
    location: &Path,
    expected_bytes: &[u8],
) -> anyhow::Result<()> {
    let actual_bytes = client
        .get(location)
        .await
        .context("failed to read remote manifest")?
        .bytes()
        .await
        .context("failed to read remote manifest bytes")?;
    anyhow::ensure!(
        actual_bytes.as_ref() == expected_bytes,
        "remote manifest bytes do not match the uploaded payload"
    );
    Ok(())
}

pub enum OptionalStateUploader {
    StateUploader(StateUploader),
    BlackHole,
}

impl OptionalStateUploader {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        match self {
            OptionalStateUploader::StateUploader(uploader) => uploader.run().await,
            OptionalStateUploader::BlackHole => std::future::pending().await,
        }
    }
}
