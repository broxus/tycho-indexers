use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore, WriteMultipart};
use tycho_core::block_strider::{
    PsCompletionContext, PsCompletionHandler, PsCompletionSubscriber, S3FileKind,
};
use tycho_core::storage::{CoreStorage, PersistentStateKind};
use tycho_util::metrics::HistogramGuard;

use crate::config::{S3Provider, StateUploaderConfig};
use crate::subscribers::CHUNK_SIZE;

pub struct StateUploader {
    config: StateUploaderConfig,
    s3_storage: Arc<DynObjectStore>,
}

impl StateUploader {
    pub fn new(config: StateUploaderConfig) -> anyhow::Result<Self> {
        let s3_storage: Arc<DynObjectStore> = match &config.provider {
            S3Provider::Aws {
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
            } => Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(&config.bucket_name)
                    .with_endpoint(endpoint)
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key)
                    .with_client_options(
                        object_store::ClientOptions::new().with_allow_http(*allow_http),
                    )
                    .build()?,
            ),
            S3Provider::Gcs { credentials_path } => Arc::new(
                GoogleCloudStorageBuilder::new()
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_connect_timeout_disabled()
                            .with_timeout_disabled(),
                    )
                    .with_bucket_name(&config.bucket_name)
                    .with_application_credentials(credentials_path)
                    .build()?,
            ),
        };

        Ok(StateUploader { config, s3_storage })
    }

    async fn on_state_persisted(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        let labels = [("workchain", cx.block_id.shard.workchain().to_string())];

        let histogram = HistogramGuard::begin("tycho_uploader_upload_persistent_state_time");

        tracing::info!("started");
        let guard = scopeguard::guard((), |_| {
            tracing::warn!("cancelled");
        });

        // Upload persistent state to S3
        self.upload_state(cx).await?;

        // Upload block data to S3
        self.upload_block_data(&cx).await?;

        // Upload block proof to S3
        self.upload_block_proof(&cx).await?;

        // Upload queue diff to S3
        self.upload_queue_diff(&cx).await?;

        metrics::gauge!("tycho_uploader_last_uploaded_state_seqno", &labels)
            .set(cx.block_id.seqno as f64);

        // Done
        scopeguard::ScopeGuard::into_inner(guard);
        tracing::info!(
            elapsed = %humantime::format_duration(histogram.finish()),
            "finished"
        );

        Ok(())
    }

    async fn upload_state(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        let kind = match cx.kind {
            PersistentStateKind::Shard => "Persistent Shard State",
            PersistentStateKind::Queue => "Persistent Queue State",
        };

        let location = match cx.kind {
            PersistentStateKind::Shard => S3FileKind::ShardState.make_path(&cx.block_id),
            PersistentStateKind::Queue => S3FileKind::QueueState.make_path(&cx.block_id),
        };

        if !self.config.enable_duplication {
            if check_exists(&self.s3_storage, &location, kind).await? {
                return Ok(());
            }
        }

        let state_info = cx
            .storage
            .persistent_state_storage()
            .get_state_info(&cx.block_id, cx.kind)
            .context("persistent state not found")?;

        let total_size = state_info.size.get();
        let chunk_size = state_info.chunk_size.get() as usize;

        let mut attempts = 0;

        // Block until we successfully upload
        'upload_loop: loop {
            attempts += 1;
            tracing::info!(attempt = attempts, "starting upload {kind}");

            let upload = match self
                .s3_storage
                .put_multipart(&Path::from(location.display().to_string()))
                .await
            {
                Ok(upload) => upload,
                Err(e) => {
                    tracing::error!(attempts, "failed to initialize multipart upload: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            // Buffer for MD5 hashes for all chunks
            let mut md5_buffer = vec![];

            let mut uploaded = 0;
            let mut offset = 0u64;

            let mut writer = WriteMultipart::new_with_chunk_size(upload, CHUNK_SIZE);

            let mut part_len = 0usize;
            let mut part_hasher = md5::Context::new();

            // Read state in chunks and write to S3
            while offset < total_size {
                // Read chunk from persistent state storage
                let state_chunk = match cx
                    .storage
                    .persistent_state_storage()
                    .read_state_part(&cx.block_id, offset, cx.kind)
                    .await
                {
                    Some(chunk) => chunk,
                    None => {
                        tracing::error!(attempts, offset, "failed to read state part");
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
                        tracing::error!(attempts, "failed to acquire upload capacity: {e}");
                        tokio::time::sleep(self.config.retry_delay).await;

                        continue 'upload_loop;
                    }

                    let remaining_in_part = CHUNK_SIZE - part_len;
                    let remaining_in_chunk = state_chunk.len() - chunk_offset;

                    let to_copy = remaining_in_chunk.min(remaining_in_part);

                    let slice = &state_chunk[chunk_offset..chunk_offset + to_copy];

                    writer.write(slice);
                    part_hasher.consume(slice);

                    part_len += slice.len();
                    chunk_offset += slice.len();

                    // If we filled the S3 part, finalize it
                    if part_len == CHUNK_SIZE {
                        let digest =
                            std::mem::replace(&mut part_hasher, md5::Context::new()).finalize();
                        md5_buffer.extend_from_slice(digest.0.as_slice());
                        part_len = 0;
                    }
                }

                uploaded += state_chunk.len();

                // Next storage chunk
                offset += chunk_size as u64;
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
                        tracing::info!(block_id = ?cx.block_id, uploaded, "upload {kind} completed successfully");
                        break;
                    }

                    tracing::error!(
                        attempt = attempts,
                        expected = expected_etag,
                        received = ?result.e_tag,
                        "ETag mismatch detected"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                }
                Err(e) => {
                    tracing::error!(attempts, "failed to complete upload {kind}: {e:?}");
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Ok(())
    }

    async fn upload_block_data(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        let kind = "Block Data";
        let location = S3FileKind::BlockData.make_path(&cx.block_id);

        if !self.config.enable_duplication {
            if check_exists(&self.s3_storage, &location, kind).await? {
                return Ok(());
            }
        }

        let handle = cx
            .storage
            .block_handle_storage()
            .load_handle(&cx.block_id)
            .context("block handle not found")?;

        let total_size = cx
            .storage
            .block_storage()
            .get_compressed_block_data_size(&handle)?
            .context("block data not found")?;

        let mut attempts = 0;

        // Block until we successfully upload
        'upload_loop: loop {
            attempts += 1;
            tracing::info!(attempt = attempts, "starting upload {kind}");

            let upload = match self
                .s3_storage
                .put_multipart(&Path::from(location.display().to_string()))
                .await
            {
                Ok(upload) => upload,
                Err(e) => {
                    tracing::error!(attempts, "failed to initialize multipart upload: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            // Buffer for MD5 hashes for all chunks
            let mut md5_buffer = vec![];

            let mut uploaded = 0;
            let mut offset = 0u64;

            let mut writer = WriteMultipart::new_with_chunk_size(upload, CHUNK_SIZE);

            // Read state in chunks and write to S3
            while offset < total_size {
                // Wait for capacity before starting a new S3 part
                if let Err(e) = writer.wait_for_capacity(self.config.max_concurrency).await {
                    tracing::error!(attempts, "failed to acquire upload capacity: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;

                    continue 'upload_loop;
                }

                // Read chunk from archive storage
                let chunk = match cx
                    .storage
                    .block_storage()
                    .load_block_data_range(&handle, offset, CHUNK_SIZE as u64)
                    .await
                {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => anyhow::bail!("block data not found"),
                    Err(e) => {
                        tracing::error!(attempts, offset, "failed to load {kind}: {e}");
                        tokio::time::sleep(self.config.retry_delay).await;

                        continue 'upload_loop;
                    }
                };

                // Write chunk to S3
                writer.write(&chunk);

                // Calculate MD5 chunk
                md5_buffer.extend_from_slice(md5::compute(&chunk).as_slice());

                uploaded += chunk.len();

                // Next chunk
                offset += CHUNK_SIZE as u64;
            }

            match writer.finish().await {
                Ok(result) => {
                    let expected_etag = hex::encode(md5::compute(&md5_buffer).as_slice());

                    if result
                        .e_tag
                        .as_deref()
                        .is_some_and(|tag| tag.trim_matches('"').starts_with(&expected_etag))
                    {
                        tracing::info!(block_id = ?cx.block_id, uploaded, "upload {kind} completed successfully");
                        break;
                    }

                    tracing::error!(
                        attempt = attempts,
                        expected = expected_etag,
                        received = ?result.e_tag,
                        "ETag mismatch detected"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                }
                Err(e) => {
                    tracing::error!(attempts, "failed to complete upload {kind}: {e:?}");
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Ok(())
    }

    async fn upload_block_proof(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        let location = S3FileKind::BlockProof.make_path(&cx.block_id);
        let handle = cx
            .storage
            .block_handle_storage()
            .load_handle(&cx.block_id)
            .context("block handle not found")?;

        let data = cx
            .storage
            .block_storage()
            .load_block_proof_raw(&handle)
            .await?;

        self.upload_bytes_data(cx, data, "Block Proof", location)
            .await
    }

    async fn upload_queue_diff(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        let location = S3FileKind::QueueDiff.make_path(&cx.block_id);
        let handle = cx
            .storage
            .block_handle_storage()
            .load_handle(&cx.block_id)
            .context("block handle not found")?;

        let data = cx
            .storage
            .block_storage()
            .load_queue_diff_raw(&handle)
            .await?;

        self.upload_bytes_data(cx, data, "Queue Diff", location)
            .await
    }

    async fn upload_bytes_data(
        &self,
        cx: &PsCompletionContext<'_>,
        data: Bytes,
        kind: &str,
        location: std::path::PathBuf,
    ) -> anyhow::Result<()> {
        if !self.config.enable_duplication {
            if check_exists(&self.s3_storage, &location, kind).await? {
                return Ok(());
            }
        }

        let total_size = data.len() as u64;
        let mut attempts = 0;

        'upload_loop: loop {
            attempts += 1;
            tracing::info!(attempt = attempts, "starting upload {kind}");

            let upload = match self
                .s3_storage
                .put_multipart(&Path::from(location.display().to_string()))
                .await
            {
                Ok(upload) => upload,
                Err(e) => {
                    tracing::error!(attempts, "failed to initialize multipart upload: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut md5_buffer = vec![];
            let mut uploaded = 0;
            let mut offset = 0u64;

            let mut writer = WriteMultipart::new_with_chunk_size(upload, CHUNK_SIZE);

            while offset < total_size {
                if let Err(e) = writer.wait_for_capacity(self.config.max_concurrency).await {
                    tracing::error!(attempts, "failed to acquire upload capacity: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue 'upload_loop;
                }

                let end = (offset + CHUNK_SIZE as u64).min(total_size);
                let chunk = data.slice(offset as usize..end as usize);

                writer.write(&chunk);
                md5_buffer.extend_from_slice(md5::compute(&chunk).as_slice());

                uploaded += chunk.len();
                offset += CHUNK_SIZE as u64;
            }

            match writer.finish().await {
                Ok(result) => {
                    let expected_etag = hex::encode(md5::compute(&md5_buffer).as_slice());

                    if result
                        .e_tag
                        .as_deref()
                        .is_some_and(|tag| tag.trim_matches('"').starts_with(&expected_etag))
                    {
                        tracing::info!(
                            block_id = ?cx.block_id,
                            uploaded,
                            "upload {kind} completed successfully",
                        );
                        break;
                    }

                    tracing::error!(
                        attempt = attempts,
                        expected = expected_etag,
                        received = ?result.e_tag,
                        "ETag mismatch detected"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                }
                Err(e) => {
                    tracing::error!(attempts, "failed to complete upload {kind}: {e:?}");
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Ok(())
    }

    // Finish uploading the last state if not yet
    pub async fn finish_last_state(&self, storage: &CoreStorage) -> anyhow::Result<()> {
        let node_state = storage.node_state();
        if let Some(block_id) = node_state.load_last_mc_block_id() {
            let block_handle_storage = storage.block_handle_storage();
            if let Some(handle) =
                block_handle_storage.find_prev_persistent_key_block(block_id.seqno)
            {
                tracing::info!(block_id = ?block_id, "finishing last state");

                if handle.has_persistent_shard_state() {
                    let cx = PsCompletionContext {
                        storage,
                        block_id: handle.id(),
                        kind: PersistentStateKind::Shard,
                    };
                    self.on_state_persisted(&cx).await?;
                }

                if handle.has_persistent_queue_state() {
                    let cx = PsCompletionContext {
                        storage,
                        block_id: handle.id(),
                        kind: PersistentStateKind::Queue,
                    };
                    self.on_state_persisted(&cx).await?;
                }
            }
        }

        Ok(())
    }
}

impl PsCompletionSubscriber for StateUploader {
    type OnStatePersistedFut<'a> = BoxFuture<'a, anyhow::Result<()>>;

    fn on_state_persisted<'a>(
        &'a self,
        cx: &'a PsCompletionContext<'_>,
    ) -> Self::OnStatePersistedFut<'a> {
        async move {
            self.on_state_persisted(cx).await?;
            Ok(())
        }
        .boxed()
    }
}

#[async_trait::async_trait]
impl PsCompletionHandler for StateUploader {
    async fn on_state_persisted(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        self.on_state_persisted(cx).await
    }
}

#[expect(clippy::large_enum_variant, reason = "doesn't matter")]
pub enum OptionalStateSubscriber {
    StateUploader(StateUploader),
    BlackHole,
}

impl PsCompletionSubscriber for OptionalStateSubscriber {
    type OnStatePersistedFut<'a> = futures_util::future::Either<
        <StateUploader as PsCompletionSubscriber>::OnStatePersistedFut<'a>,
        futures_util::future::Ready<anyhow::Result<()>>,
    >;

    fn on_state_persisted<'a>(
        &'a self,
        cx: &'a PsCompletionContext<'_>,
    ) -> Self::OnStatePersistedFut<'a> {
        match self {
            OptionalStateSubscriber::StateUploader(uploader) => {
                futures_util::future::Either::Left(uploader.on_state_persisted(cx).boxed())
            }
            OptionalStateSubscriber::BlackHole => {
                futures_util::future::Either::Right(futures_util::future::ok(()))
            }
        }
    }
}

#[async_trait::async_trait]
impl PsCompletionHandler for OptionalStateSubscriber {
    async fn on_state_persisted(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        match self {
            OptionalStateSubscriber::StateUploader(uploader) => {
                uploader.on_state_persisted(cx).await
            }
            OptionalStateSubscriber::BlackHole => Ok(()),
        }
    }
}

async fn check_exists(
    s3_storage: &Arc<DynObjectStore>,
    location: &std::path::PathBuf,
    kind: &str,
) -> anyhow::Result<bool> {
    match s3_storage
        .head(&Path::from(location.display().to_string()))
        .await
    {
        Ok(meta) => {
            tracing::info!(?meta, "{} already exists, skipping upload", kind);
            Ok(true)
        }
        Err(object_store::Error::NotFound { path, .. }) => {
            tracing::info!(path, "{} not found, starting upload", kind);
            Ok(false)
        }
        Err(e) => {
            tracing::error!("error checking {} existence: {e}, starting upload", kind);
            Ok(false)
        }
    }
}
