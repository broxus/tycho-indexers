use std::sync::Arc;

use anyhow::Result;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_storage::Storage;

#[repr(transparent)]
pub struct NodeStats {
    inner: Arc<Inner>,
}

impl NodeStats {
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: Arc::new(Inner { storage }),
        }
    }

    fn handle_block_impl(&self, cx: &BlockSubscriberContext) {
        let last_mc_block_id = cx.mc_block_id.seqno;
        metrics::gauge!("tycho_downloader_last_mc_block_id").set(last_mc_block_id);

        let last_archive_id = self
            .inner
            .storage
            .block_storage()
            .list_archive_ids()
            .last()
            .cloned();

        metrics::gauge!("tycho_downloader_last_archive_id")
            .set(last_archive_id.unwrap_or_default());
    }
}

impl Clone for NodeStats {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl BlockSubscriber for NodeStats {
    type Prepared = ();

    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        self.handle_block_impl(cx);
        futures_util::future::ready(Ok(()))
    }
}

struct Inner {
    storage: Storage,
}
