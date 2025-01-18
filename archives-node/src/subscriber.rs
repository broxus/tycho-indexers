use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{ObjectStore, WriteMultipart};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_core::block_strider::{ArchiveSubscriber, ArchiveSubscriberContext};
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;

use crate::config::ArchiveUploaderConfig;

pub struct ArchiveUploader {
    gcs: Arc<GoogleCloudStorage>,
    config: ArchiveUploaderConfig,
    prev_archive_upload: tokio::sync::Mutex<Option<UploadArchiveTask>>,
}

impl ArchiveUploader {
    pub fn new(config: ArchiveUploaderConfig) -> Result<Self> {
        let gcs = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name(&config.bucket_name)
                .with_application_credentials(&config.credentials_path)
                .build()?,
        );

        Ok(ArchiveUploader {
            gcs,
            config,
            prev_archive_upload: Default::default(),
        })
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
            let gcs = self.gcs.clone();
            let archive_id = cx.archive_id;
            let storage = cx.storage.clone();
            let location = cx.archive_id.to_string();
            let retry_delay = self.config.retry_delay;

            let cancelled = cancelled.clone();

            async move {
                let histogram = HistogramGuard::begin("tycho_storage_upload_archive_time");

                tracing::info!("started");
                let guard = scopeguard::guard((), |_| {
                    tracing::warn!("cancelled");
                });

                let mut attempts = 0;

                // Try to load until we load without retry limit
                loop {
                    attempts += 1;

                    let upload = match gcs.put_multipart(&Path::from(location.clone())).await {
                        Ok(upload) => upload,
                        Err(e) => {
                            tracing::error!(attempts, "failed to put multipart: {e}");
                            tokio::time::sleep(retry_delay).await;
                            continue;
                        }
                    };

                    let mut writer = WriteMultipart::new(upload);
                    for (_, chunk) in storage.block_storage().archive_chunks_iterator(archive_id) {
                        anyhow::ensure!(!cancelled.check(), "task aborted");
                        writer.write(&chunk);
                    }

                    match writer.finish().await {
                        Ok(_) => break,
                        Err(e) => {
                            tracing::error!(attempts, "failed to upload archive: {e}");
                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }

                // Done
                scopeguard::ScopeGuard::into_inner(guard);
                tracing::info!(
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
                    "failed to commit archive: {e:?}"
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
