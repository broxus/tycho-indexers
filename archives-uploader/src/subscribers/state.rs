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

mod state_upload_plan;

use state_upload_plan::{StateObjectRole, StateUploadPlan, UploadAction};

pub struct StateUploader {
    inner: Arc<Inner>,

    run_handle: Option<tokio::task::JoinHandle<()>>,
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

        Ok(StateUploader {
            inner,
            run_handle: None,
        })
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        if let Some(handle) = &self.run_handle
            && !handle.is_finished()
        {
            anyhow::bail!("state uploader already running");
        }

        let inner = self.inner.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("state uploader started");
            if let Err(e) = inner.run().await {
                tracing::error!(%e, "state uploader failed");
            }
            tracing::info!("state uploader finished");
        });

        self.run_handle = Some(handle);

        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.run_handle.take() {
            handle.abort();
        }
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
            (&main_location, state_info.size.get()),
            &manifest_location,
            meta.as_ref(),
            &parts,
            self.config.enable_duplication,
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
        let mut attempt = 0;
        loop {
            attempt += 1;
            tracing::info!(%role, declared_size, attempt, "starting state upload");
            match self
                .s3_client
                .client()
                .put(
                    location,
                    PutPayload::from_bytes(Bytes::copy_from_slice(bytes)),
                )
                .await
            {
                Ok(_) => match verify_uploaded_manifest_bytes(
                    self.s3_client.client().as_ref(),
                    location,
                    bytes,
                )
                .await
                {
                    Ok(()) => {
                        tracing::info!(%role, declared_size, attempt, "state manifest upload completed successfully");
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::error!(%role, declared_size, attempt, "failed to verify uploaded state manifest: {e:#}");
                    }
                },
                Err(e) => {
                    tracing::error!(%role, declared_size, attempt, "failed to upload state manifest: {e:#}");
                }
            }
            tokio::time::sleep(self.config.retry_delay).await;
        }
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

        let mut attempts = 0;

        // Block until we successfully upload
        'upload_loop: loop {
            attempts += 1;
            tracing::info!(
                attempt = attempts,
                %role,
                declared_size = total_size,
                ?block_id,
                ?kind,
                "starting state upload"
            );

            let upload = match s3_client.put_multipart(&location).await {
                Ok(upload) => upload,
                Err(e) => {
                    tracing::error!(
                        attempts,
                        %role,
                        declared_size = total_size,
                        ?block_id,
                        ?kind,
                        "failed to initialize multipart upload: {e}"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

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
                let state_chunk = match storage
                    .persistent_state_storage()
                    .read_state_chunk(block_id, offset, kind, prefix)
                    .await
                {
                    Some(chunk) => chunk,
                    None => {
                        tracing::error!(
                            attempts,
                            %role,
                            declared_size = total_size,
                            ?block_id,
                            ?kind,
                            offset,
                            "failed to read state chunk"
                        );
                        tokio::time::sleep(self.config.retry_delay).await;

                        continue 'upload_loop;
                    }
                };

                // Process the chunk byte by byte, accumulating into S3 parts
                let mut chunk_offset = 0;
                while chunk_offset < state_chunk.len() {
                    // Wait for capacity before starting a new S3 part
                    if part_len == 0
                        && let Err(e) = writer.wait_for_capacity(self.config.max_concurrency).await
                    {
                        tracing::error!(
                            attempts,
                            %role,
                            declared_size = total_size,
                            ?block_id,
                            ?kind,
                            "failed to acquire upload state capacity: {e}"
                        );
                        tokio::time::sleep(self.config.retry_delay).await;

                        continue 'upload_loop;
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

            match writer.finish().await {
                Ok(result) => {
                    let expected_etag = hex::encode(md5::compute(&md5_buffer).as_slice());

                    if result
                        .e_tag
                        .as_deref()
                        .is_some_and(|tag| tag.trim_matches('"').starts_with(&expected_etag))
                    {
                        tracing::info!(block_id = ?block_id, ?kind, %role, declared_size = total_size, attempts, uploaded, "upload state object completed successfully");
                        break;
                    }

                    tracing::error!(
                        attempt = attempts,
                        %role,
                        declared_size = total_size,
                        ?block_id,
                        ?kind,
                        expected = expected_etag,
                        received = ?result.e_tag,
                        "state ETag mismatch detected"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                }
                Err(e) => {
                    tracing::error!(
                        attempts,
                        %role,
                        declared_size = total_size,
                        ?block_id,
                        ?kind,
                        "failed to complete state upload: {e:?}"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Ok(())
    }
}

async fn verify_existing_manifest(
    client: &dyn ObjectStore,
    location: &Path,
    expected_meta: &PersistentStateMeta,
) -> anyhow::Result<()> {
    let actual_bytes = client
        .get(location)
        .await
        .context("failed to read remote manifest")?
        .bytes()
        .await
        .context("failed to read remote manifest bytes")?;
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
    pub fn run(&mut self) -> anyhow::Result<()> {
        match self {
            OptionalStateUploader::StateUploader(uploader) => uploader.run(),
            OptionalStateUploader::BlackHole => Ok(()),
        }
    }

    pub fn stop(&mut self) {
        match self {
            OptionalStateUploader::StateUploader(uploader) => uploader.stop(),
            OptionalStateUploader::BlackHole => {}
        }
    }
}
