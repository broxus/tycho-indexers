use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{ObjectStore, WriteMultipart};
use tycho_core::block_strider::{ArchiveSubscriber, ArchiveSubscriberContext};

use crate::config::ArchiveUploaderConfig;

pub struct ArchiveUploader {
    config: ArchiveUploaderConfig,
    gcs: GoogleCloudStorage,
}

impl ArchiveUploader {
    pub fn new(config: ArchiveUploaderConfig) -> Result<Self> {
        let gcs = GoogleCloudStorageBuilder::new()
            .with_bucket_name("archives")
            .with_application_credentials(&config.credentials_path)
            .build()?;

        Ok(ArchiveUploader { config, gcs })
    }

    async fn handle_archive(&self, cx: &ArchiveSubscriberContext<'_>) -> Result<()> {
        let archive_id = cx.archive_id.to_string();
        let instance_id = hex::encode(cx.storage.node_state().load_instance_id());

        let location = format!("{instance_id}/{archive_id}");

        let mut attempts = 0;

        // Try to load until we load without retry limit
        loop {
            attempts += 1;

            let upload = match self.gcs.put_multipart(&Path::from(location.clone())).await {
                Ok(upload) => upload,
                Err(e) => {
                    tracing::error!(attempts, "failed to put multipart: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut writer = WriteMultipart::new(upload);
            for (_, chunk) in cx.archive_iterator() {
                writer.write(&chunk);
            }

            match writer.finish().await {
                Ok(_) => break,
                Err(e) => {
                    tracing::error!(attempts, "failed to upload archive: {e}");
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Ok(())
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

#[expect(clippy::large_enum_variant, reason = "doesn't matter")]
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
