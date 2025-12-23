use std::sync::Arc;

use anyhow::Context;
use object_store::{ObjectStore, WriteMultipart};
use tycho_core::storage::{CoreStorage, PersistentState};
use tycho_core_bridge::S3Client;
use tycho_util::metrics::HistogramGuard;

use crate::config::UploaderConfig;

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
        let s3_chunk_size = self.s3_client.chunk_size();

        let block_id = state.block_id();
        let kind = state.kind();

        let location = self.s3_client.make_state_key(block_id, kind);

        if !self.config.enable_duplication {
            // Check state existence before upload
            match s3_client.head(&location).await {
                Ok(meta) => {
                    tracing::info!(
                        ?meta,
                        ?block_id,
                        ?kind,
                        "state already exists, skipping upload"
                    );
                    return Ok(());
                }
                Err(object_store::Error::NotFound { path, .. }) => {
                    tracing::info!(path, ?block_id, ?kind, "state not found, starting upload");
                }
                Err(e) => {
                    tracing::error!(
                        ?block_id,
                        ?kind,
                        "error checking state existence: {e}, starting upload"
                    );
                }
            }
        }

        let state_info = storage
            .persistent_state_storage()
            .get_state_info(block_id, kind)
            .context("persistent state not found")?;

        let total_size = state_info.size.get();

        let mut attempts = 0;

        // Block until we successfully upload
        'upload_loop: loop {
            attempts += 1;
            tracing::info!(
                attempt = attempts,
                ?block_id,
                ?kind,
                "starting state upload"
            );

            let upload = match s3_client.put_multipart(&location).await {
                Ok(upload) => upload,
                Err(e) => {
                    tracing::error!(
                        attempts,
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
                    .read_state_part(block_id, offset, kind)
                    .await
                {
                    Some(chunk) => chunk,
                    None => {
                        tracing::error!(
                            attempts,
                            ?block_id,
                            ?kind,
                            offset,
                            "failed to read state part"
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
                        tracing::info!(block_id = ?block_id, ?kind, uploaded, "upload state completed successfully");
                        break;
                    }

                    tracing::error!(
                        attempt = attempts,
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
