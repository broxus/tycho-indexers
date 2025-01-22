use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore, WriteMultipart};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_core::block_strider::{ArchiveSubscriber, ArchiveSubscriberContext};
use tycho_storage::Storage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;

use crate::config::{ArchiveUploaderConfig, S3Provider};

pub struct ArchiveUploader {
    config: ArchiveUploaderConfig,
    s3_storage: Arc<DynObjectStore>,
    prev_archive_upload: tokio::sync::Mutex<Option<UploadArchiveTask>>,
}

impl ArchiveUploader {
    pub fn new(config: ArchiveUploaderConfig) -> Result<Self> {
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

        Ok(ArchiveUploader {
            config,
            s3_storage,
            prev_archive_upload: Default::default(),
        })
    }

    // Finish upload last committed archives
    async fn upload_committed_archives(&self, storage: &Storage) -> Result<()> {
        let block_storage = storage.block_storage();

        let archive_ids = block_storage.list_archive_ids();

        let start = archive_ids
            .len()
            .saturating_sub(self.config.last_archives_to_upload);

        for archive_id in archive_ids[start..].iter() {
            // Check archive is committed
            if block_storage.get_archive_size(*archive_id)?.is_some() {
                self.handle_archive(&ArchiveSubscriberContext {
                    archive_id: *archive_id,
                    storage,
                })
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_archive(&self, cx: &ArchiveSubscriberContext<'_>) -> Result<()> {
        let mut prev_archive_upload = self.prev_archive_upload.lock().await;

        // NOTE: Wait on reference to make sure that the task is cancel safe
        if let Some(task) = &mut *prev_archive_upload {
            // Wait upload archive
            task.finish().await?;
        }
        *prev_archive_upload = Some(self.spawn_upload_archive(cx));

        Ok(())
    }

    fn spawn_upload_archive(&self, cx: &ArchiveSubscriberContext<'_>) -> UploadArchiveTask {
        let cancelled = CancellationFlag::new();

        let handle = tokio::task::spawn({
            let archive_id = cx.archive_id;
            let storage = cx.storage.clone();
            let location = cx.archive_id.to_string();

            let retry_delay = self.config.retry_delay;
            let chunk_size = self.config.chunk_size.as_u64() as _;
            let max_concurrency = self.config.max_concurrency;
            let s3_storage = self.s3_storage.clone();

            let cancelled = cancelled.clone();

            async move {
                let histogram = HistogramGuard::begin("tycho_storage_upload_archive_time");

                tracing::info!("started");
                let guard = scopeguard::guard((), |_| {
                    tracing::warn!("cancelled");
                });

                let mut attempts = 0;

                // Block the strider until we successfully upload
                loop {
                    attempts += 1;
                    tracing::info!(attempt = attempts, "starting upload archive");

                    let upload = match s3_storage
                        .put_multipart(&Path::from(location.clone()))
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

                    let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
                    for (_, chunk) in storage.block_storage().archive_chunks_iterator(archive_id) {
                        anyhow::ensure!(!cancelled.check(), "task aborted");

                        if let Err(e) = writer.wait_for_capacity(max_concurrency).await {
                            tracing::error!(attempts, "failed to acquire upload capacity: {e}");
                            tokio::time::sleep(retry_delay).await;

                            continue;
                        }

                        // Write chunk
                        writer.write(&chunk);

                        // Calculate MD5 chunk
                        md5_buffer.extend_from_slice(md5::compute(&chunk).as_slice());
                    }

                    match writer.finish().await {
                        Ok(result) => {
                            let expected_etag = hex::encode(md5::compute(&md5_buffer).as_slice());

                            if result.e_tag.as_deref().is_some_and(|tag| {
                                tag.trim_matches('"').starts_with(&expected_etag)
                            }) {
                                tracing::info!("upload archive completed successfully");
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
                            tracing::error!(attempts, "failed to complete upload archive: {e}");
                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }

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
                "spawn_upload_archive",
                archive_id = cx.archive_id
            ))
        });

        UploadArchiveTask {
            cancelled,
            handle: Some(handle),
            archive_id: cx.archive_id,
        }
    }
}

impl ArchiveSubscriber for ArchiveUploader {
    type HandleArchiveFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a> {
        async move {
            self.handle_archive(cx).await?;
            Ok(())
        }
        .boxed()
    }
}

pub enum OptionalArchiveSubscriber {
    ArchiveUploader(ArchiveUploader),
    BlackHole,
}

impl OptionalArchiveSubscriber {
    pub async fn upload_committed_archives(&self, storage: &Storage) -> Result<()> {
        match self {
            OptionalArchiveSubscriber::ArchiveUploader(uploader) => {
                uploader.upload_committed_archives(storage).await
            }
            OptionalArchiveSubscriber::BlackHole => Ok(()),
        }
    }
}

impl ArchiveSubscriber for OptionalArchiveSubscriber {
    type HandleArchiveFut<'a> = futures_util::future::Either<
        <ArchiveUploader as ArchiveSubscriber>::HandleArchiveFut<'a>,
        futures_util::future::Ready<Result<()>>,
    >;

    fn handle_archive<'a>(
        &'a self,
        cx: &'a ArchiveSubscriberContext<'_>,
    ) -> Self::HandleArchiveFut<'a> {
        match self {
            OptionalArchiveSubscriber::ArchiveUploader(uploader) => {
                futures_util::future::Either::Left(uploader.handle_archive(cx).boxed())
            }
            OptionalArchiveSubscriber::BlackHole => {
                futures_util::future::Either::Right(futures_util::future::ok(()))
            }
        }
    }
}

struct UploadArchiveTask {
    archive_id: u32,
    cancelled: CancellationFlag,
    handle: Option<JoinHandle<Result<()>>>,
}

impl UploadArchiveTask {
    async fn finish(&mut self) -> Result<()> {
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
                    archive_id = self.archive_id,
                    "failed to upload archive: {e:?}"
                );
            }

            self.handle = None;
        }

        Ok(())
    }
}

impl Drop for UploadArchiveTask {
    fn drop(&mut self) {
        self.cancelled.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}
