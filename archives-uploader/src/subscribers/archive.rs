use anyhow::{Context, Result};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStore, WriteMultipart};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tycho_core::block_strider::{ArchiveSubscriber, ArchiveSubscriberContext};
use tycho_core::storage::CoreStorage;
use tycho_core_bridge::S3Client;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::CancellationFlag;

use crate::config::UploaderConfig;

pub struct ArchiveUploader {
    config: UploaderConfig,
    s3_client: S3Client,
    prev_archive_upload: tokio::sync::Mutex<Option<UploadArchiveTask>>,
}

impl ArchiveUploader {
    pub fn new(config: UploaderConfig, s3_client: S3Client) -> Result<Self> {
        Ok(ArchiveUploader {
            config,
            s3_client,
            prev_archive_upload: Default::default(),
        })
    }

    // Finish upload last committed archives
    pub async fn upload_committed_archives(&self, storage: &CoreStorage) -> Result<()> {
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
            let location = self.s3_client.make_archive_key(archive_id).to_string();

            let retry_delay = self.config.retry_delay;
            let max_concurrency = self.config.max_concurrency;
            let enable_duplication = self.config.enable_duplication;

            let storage = cx.storage.clone();
            let s3_client = self.s3_client.clone();
            let s3_chunk_size = s3_client.chunk_size();

            let cancelled = cancelled.clone();

            async move {
                let histogram = HistogramGuard::begin("tycho_uploader_upload_archive_time");

                tracing::info!("started");
                let guard = scopeguard::guard((), |_| {
                    tracing::warn!("cancelled");
                });

                // Check archive existence before upload
                if !enable_duplication {
                    match s3_client.client().head(&Path::from(location.clone())).await {
                        Ok(meta) => {
                            tracing::info!(?meta, "archive already exists, skipping upload");
                            return Ok(());
                        }
                        Err(object_store::Error::NotFound { path, .. }) => {
                            tracing::info!(path, "archive not found, starting upload");
                        }
                        Err(e) => {
                            tracing::error!(
                                "error checking archive existence: {e}, starting upload"
                            );
                        }
                    }
                }

                // Get archive info
                let archive_size = storage
                    .block_storage()
                    .get_archive_size(archive_id)?
                    .context("archive not found")?;

                let mut attempts = 0;

                // Block until we successfully upload
                'upload_loop: loop {
                    attempts += 1;
                    tracing::info!(attempt = attempts, "starting upload archive");

                    let upload = match s3_client
                        .client()
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

                    // Buffer for MD5 hashes for all S3 parts
                    let mut md5_buffer = vec![];

                    let mut uploaded = 0;
                    let mut offset = 0u64;

                    let mut writer = WriteMultipart::new_with_chunk_size(upload, s3_chunk_size);

                    let mut part_len = 0usize;
                    let mut part_hasher = md5::Context::new();

                    // Read archive in chunks and accumulate into S3 parts
                    while offset < archive_size as u64 {
                        anyhow::ensure!(!cancelled.check(), "task aborted");

                        // Read chunk from archive storage
                        let archive_chunk = match storage
                            .block_storage()
                            .get_archive_chunk(archive_id, offset)
                            .await
                        {
                            Ok(chunk) => chunk,
                            Err(e) => {
                                tracing::error!(
                                    attempts,
                                    offset,
                                    "failed to read archive chunk: {e}"
                                );
                                tokio::time::sleep(retry_delay).await;

                                continue 'upload_loop;
                            }
                        };

                        // Process the chunk byte by byte, accumulating into S3 parts
                        let mut chunk_offset = 0;
                        while chunk_offset < archive_chunk.len() {
                            anyhow::ensure!(!cancelled.check(), "task aborted");

                            // Wait for capacity before starting a new S3 part
                            if part_len == 0
                                && let Err(e) = writer.wait_for_capacity(max_concurrency).await
                            {
                                tracing::error!(attempts, "failed to acquire upload capacity: {e}");
                                tokio::time::sleep(retry_delay).await;

                                continue 'upload_loop;
                            }

                            let remaining_in_part = s3_chunk_size - part_len;
                            let remaining_in_chunk = archive_chunk.len() - chunk_offset;

                            let to_copy = remaining_in_chunk.min(remaining_in_part);

                            let slice = &archive_chunk[chunk_offset..chunk_offset + to_copy];

                            writer.write(slice);
                            part_hasher.consume(slice);

                            part_len += slice.len();
                            chunk_offset += slice.len();

                            // If we filled the S3 part, finalize it
                            if part_len == s3_chunk_size {
                                let digest =
                                    std::mem::replace(&mut part_hasher, md5::Context::new())
                                        .finalize();
                                md5_buffer.extend_from_slice(digest.0.as_slice());
                                part_len = 0;
                            }
                        }

                        uploaded += archive_chunk.len();

                        // Next archive chunk
                        offset += archive_chunk.len() as u64;
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
                                tracing::info!("upload archive completed successfully");

                                metrics::histogram!("tycho_uploader_archive_size")
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
                            tracing::error!(attempts, "failed to complete upload archive: {e:?}");
                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }

                metrics::gauge!("tycho_uploader_last_uploaded_archive_seqno").set(archive_id);

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
