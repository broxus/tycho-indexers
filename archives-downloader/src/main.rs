use anyhow::Result;
use clap::Parser;
use tycho_core::block_strider::{
    ArchiveBlockProvider, BlockStrider, MetricsSubscriber, NoopSubscriber, ShardStateApplier,
};
use tycho_core::node::CmdRunArgs;
use tycho_core_bridge::RunContext;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct ExplorerArgs {
    #[clap(flatten)]
    node: CmdRunArgs,
}

fn main() -> Result<()> {
    let args = ExplorerArgs::parse();

    tycho_core_bridge::run_node(args.node, false, |ctx: RunContext<()>| async move {
        let RunContext {
            config,
            storage,
            strider_state,
            s3_client,
            ..
        } = ctx;

        let s3_client = s3_client.ok_or_else(|| anyhow::anyhow!("s3 client not initialized"))?;

        let provider = ArchiveBlockProvider::new(
            s3_client,
            storage.clone(),
            config.base.archive_block_provider.clone(),
        );
        let applier = ShardStateApplier::new(storage.clone(), NoopSubscriber);

        let strider = BlockStrider::builder()
            .with_provider(provider)
            .with_state(strider_state)
            .with_block_subscriber((applier, MetricsSubscriber))
            .build();

        Ok(strider)
    })
}
