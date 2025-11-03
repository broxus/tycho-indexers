use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore, WriteMultipart};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_core::block_strider::{PsCompletionContext, PsCompletionHandler, PsCompletionSubscriber};
use tycho_core::storage::{CoreStorage, PersistentStateKind};
use tycho_types::models::BlockId;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;

use crate::config::{S3Provider, StateUploaderConfig};
use crate::subscribers::CHUNK_SIZE;

pub struct StateUploader {
    config: StateUploaderConfig,
    s3_storage: Arc<DynObjectStore>,
    prev_archive_upload: tokio::sync::Mutex<Option<UploadStateTask>>,
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

        Ok(StateUploader {
            config,
            s3_storage,
            prev_archive_upload: Default::default(),
        })
    }

    async fn on_state_persisted(&self, cx: &PsCompletionContext<'_>) -> anyhow::Result<()> {
        tracing::info!(block_id = ?cx.block_id, "on_state_persisted");

        let mut prev_archive_upload = self.prev_archive_upload.lock().await;

        // NOTE: Wait on reference to make sure that the task is cancel safe
        if let Some(task) = &mut *prev_archive_upload {
            // Wait upload archive
            task.finish().await?;
        }
        *prev_archive_upload = Some(self.spawn_upload_state(cx));

        Ok(())
    }

    fn spawn_upload_state(&self, cx: &PsCompletionContext<'_>) -> UploadStateTask {
        let cancelled = CancellationFlag::new();

        let handle = tokio::task::spawn({
            let block_id = *cx.block_id;
            let kind = cx.kind;

            let location = if let Some(prefix) = &self.config.location {
                PathBuf::from(prefix).join(cx.kind.make_file_name(cx.block_id))
            } else {
                cx.kind.make_file_name(cx.block_id)
            };

            let retry_delay = self.config.retry_delay;
            let max_concurrency = self.config.max_concurrency;
            let enable_duplication = self.config.enable_duplication;

            let storage = cx.storage.clone();
            let s3_storage = self.s3_storage.clone();

            let cancelled = cancelled.clone();

            async move {
                let labels = [("workchain", block_id.shard.workchain().to_string())];

                let histogram =
                    HistogramGuard::begin("tycho_uploader_upload_persistent_state_time");

                tracing::info!("started");
                let guard = scopeguard::guard((), |_| {
                    tracing::warn!("cancelled");
                });

                // Check state existence before upload to S3
                if !enable_duplication {
                    match s3_storage
                        .head(&Path::from(location.to_string_lossy().as_ref()))
                        .await
                    {
                        Ok(meta) => {
                            tracing::info!(?meta, "state already exists, skipping upload");
                            return Ok(());
                        }
                        Err(object_store::Error::NotFound { path, .. }) => {
                            tracing::info!(path, "state not found, starting upload");
                        }
                        Err(e) => {
                            tracing::error!(
                                "error checking persistent state existence: {e}, starting upload"
                            );
                        }
                    }
                }

                let state_info = storage
                    .persistent_state_storage()
                    .get_state_info(&block_id, kind)
                    .context("state not found")?;

                let total_size = state_info.size.get();
                let chunk_size = state_info.chunk_size.get() as usize;

                let mut attempts = 0;

                // Block until we successfully upload
                'upload_loop: loop {
                    attempts += 1;
                    tracing::info!(attempt = attempts, "starting upload state");

                    let upload = match s3_storage
                        .put_multipart(&Path::from(location.to_string_lossy().as_ref()))
                        .await
                    {
                        Ok(upload) => upload,
                        Err(e) => {
                            tracing::error!(attempts, "failed to initialize multipart upload: {e}");
                            tokio::time::sleep(retry_delay).await;
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
                        anyhow::ensure!(!cancelled.check(), "task aborted");

                        // Read chunk from persistent state storage
                        let state_chunk = match storage
                            .persistent_state_storage()
                            .read_state_part(&block_id, offset, kind)
                            .await
                        {
                            Some(chunk) => chunk,
                            None => {
                                tracing::error!(attempts, offset, "failed to read state part");
                                tokio::time::sleep(retry_delay).await;

                                continue 'upload_loop;
                            }
                        };

                        // Process the chunk byte by byte, accumulating into S3 parts
                        let mut chunk_offset = 0;
                        while chunk_offset < state_chunk.len() {
                            anyhow::ensure!(!cancelled.check(), "task aborted");

                            // Wait for capacity before starting a new S3 part
                            if part_len == 0
                                && let Err(e) = writer.wait_for_capacity(max_concurrency).await
                            {
                                tracing::error!(attempts, "failed to acquire upload capacity: {e}");
                                tokio::time::sleep(retry_delay).await;

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
                                    std::mem::replace(&mut part_hasher, md5::Context::new())
                                        .finalize();
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
                        let digest =
                            std::mem::replace(&mut part_hasher, md5::Context::new()).finalize();
                        md5_buffer.extend_from_slice(digest.0.as_slice());
                    }

                    match writer.finish().await {
                        Ok(result) => {
                            let expected_etag = hex::encode(md5::compute(&md5_buffer).as_slice());

                            if result.e_tag.as_deref().is_some_and(|tag| {
                                tag.trim_matches('"').starts_with(&expected_etag)
                            }) {
                                tracing::info!("upload state completed successfully");

                                metrics::histogram!("tycho_uploader_state_size", &labels)
                                    .record(uploaded as f64);

                                break;
                            }

                            tracing::error!(
                                attempt = attempts,
                                expected = expected_etag,
                                received = ?result.e_tag,
                                "ETag mismatch detected"
                            );
                            tokio::time::sleep(retry_delay).await;
                        }
                        Err(e) => {
                            tracing::error!(attempts, "failed to complete upload state: {e:?}");
                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }

                metrics::gauge!("tycho_uploader_last_uploaded_state_seqno", &labels)
                    .set(block_id.seqno as f64);

                // Done
                scopeguard::ScopeGuard::into_inner(guard);
                tracing::info!(
                    attempts,
                    elapsed = %humantime::format_duration(histogram.finish()),
                    "finished"
                );

                Ok(())
            }
            .instrument(tracing::info_span!(
                "spawn_upload_state",
                block_id = ?cx.block_id,
                kind = ?cx.kind,
            ))
        });

        UploadStateTask {
            cancelled,
            kind: cx.kind,
            block_id: *cx.block_id,
            handle: Some(handle),
        }
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
                    self.spawn_upload_state(&cx).finish().await?;
                }

                if handle.has_persistent_queue_state() {
                    let cx = PsCompletionContext {
                        storage,
                        block_id: handle.id(),
                        kind: PersistentStateKind::Queue,
                    };
                    self.spawn_upload_state(&cx).finish().await?;
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

struct UploadStateTask {
    block_id: BlockId,
    kind: PersistentStateKind,
    cancelled: CancellationFlag,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl UploadStateTask {
    async fn finish(&mut self) -> anyhow::Result<()> {
        // NOTE: Await on reference to make sure that the task is cancel safe
        if let Some(handle) = &mut self.handle {
            if let Err(e) = handle
                .await
                .map_err(|e| {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                    anyhow::Error::from(e)
                })
                .and_then(std::convert::identity)
            {
                tracing::error!(
                    block_id = ?self.block_id,
                    kind = ?self.kind,
                    "failed to upload state: {e:?}"
                );
            }

            self.handle = None;
        }

        Ok(())
    }
}

impl Drop for UploadStateTask {
    fn drop(&mut self) {
        self.cancelled.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}
