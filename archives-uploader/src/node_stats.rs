use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext};

#[repr(transparent)]
pub struct NodeStats;

impl NodeStats {
    fn handle_block_impl(&self, cx: &BlockSubscriberContext) -> anyhow::Result<()> {
        let last_mc_block_id = cx.mc_block_id.seqno;
        metrics::gauge!("tycho_uploader_last_mc_block_id").set(last_mc_block_id);

        let gen_utime = cx.block.block().load_info()?.gen_utime;
        metrics::gauge!("tycho_uploader_last_block_utime").set(gen_utime);

        Ok(())
    }
}

impl BlockSubscriber for NodeStats {
    type Prepared = ();

    type PrepareBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<anyhow::Result<()>>;

    fn prepare_block<'a>(&'a self, _: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }

    fn handle_block<'a>(
        &'a self,
        cx: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        let res = self.handle_block_impl(cx);
        futures_util::future::ready(res)
    }
}
