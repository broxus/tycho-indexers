use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::BlockStuff;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};
use tycho_storage::{BlockHandle, NewBlockMeta, Storage};

#[repr(transparent)]
pub struct BlockApplier {
    inner: Arc<Inner>,
}

impl BlockApplier {
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: Arc::new(Inner { storage }),
        }
    }

    async fn prepare_block_impl(
        &self,
        cx: &BlockSubscriberContext,
    ) -> Result<BlockApplierPrepared> {
        // Load handle
        let handle = self
            .get_block_handle(&cx.mc_block_id, &cx.block, &cx.archive_data)
            .await?;

        Ok(BlockApplierPrepared { handle })
    }

    async fn handle_block_impl(
        &self,
        cx: &BlockSubscriberContext,
        prepared: BlockApplierPrepared,
    ) -> Result<()> {
        // Mark block as applied
        self.inner
            .storage
            .block_handle_storage()
            .set_block_applied(&prepared.handle);

        if self.inner.storage.config().archives_gc.is_some() {
            tracing::debug!(block_id = %prepared.handle.id(), "saving block into archive");
            self.inner
                .storage
                .block_storage()
                .move_into_archive(&prepared.handle, cx.mc_is_key_block)
                .await?;
        }

        // Done
        Ok(())
    }

    async fn get_block_handle(
        &self,
        mc_block_id: &BlockId,
        block: &BlockStuff,
        archive_data: &ArchiveData,
    ) -> Result<BlockHandle> {
        let block_storage = self.inner.storage.block_storage();

        let info = block.load_info()?;
        let res = block_storage
            .store_block_data(
                block,
                archive_data,
                NewBlockMeta {
                    is_key_block: info.key_block,
                    gen_utime: info.gen_utime,
                    ref_by_mc_seqno: mc_block_id.seqno,
                },
            )
            .await?;

        Ok(res.handle)
    }
}

impl Clone for BlockApplier {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl BlockSubscriber for BlockApplier {
    type Prepared = BlockApplierPrepared;

    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        Box::pin(self.prepare_block_impl(cx))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(self.handle_block_impl(cx, prepared))
    }
}

pub struct BlockApplierPrepared {
    handle: BlockHandle,
}

struct Inner {
    storage: Storage,
}
