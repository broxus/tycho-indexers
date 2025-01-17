use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_core::block_strider::{ArchiveSubscriber, ArchiveSubscriberContext};

use crate::config::ArchiveUploaderConfig;

pub struct ArchiveUploader {
    _config: ArchiveUploaderConfig,
}

impl ArchiveUploader {
    pub fn new(config: ArchiveUploaderConfig) -> Result<Self> {
        Ok(ArchiveUploader { _config: config })
    }

    async fn handle_archive(&self, _cx: &ArchiveSubscriberContext<'_>) -> Result<()> {
        // TODO

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
            self.handle_archive(&cx).await?;
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
                futures_util::future::Either::Left(uploader.handle_archive(&cx).boxed())
            }
            OptionalArchiveSubscriber::BlackHole => {
                futures_util::future::Either::Right(futures_util::future::ok(()))
            }
        }
    }
}
