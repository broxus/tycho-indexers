use anyhow::{Context, Result};
use clap::Parser;
use tycho_core::block_strider::{
    ArchiveHandler, BlockStrider, MetricsSubscriber, ShardStateApplier,
};
use tycho_core::node::CmdRunArgs;

use crate::config::UserConfig;
use crate::subscribers::{
    ArchiveUploader, OptionalArchiveSubscriber, OptionalStateUploader, StateUploader,
};

mod config;
mod subscribers;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct ExplorerArgs {
    #[clap(flatten)]
    node: CmdRunArgs,
}

fn main() -> Result<()> {
    let args = ExplorerArgs::parse();

    tycho_core_bridge::run_node(
        args.node,
        true,
        |ctx: tycho_core_bridge::RunContext<UserConfig>| async move {
            let tycho_core_bridge::RunContext {
                node,
                config,
                storage,
                strider_state,
                rpc_blocks,
                rpc_states,
                s3_client,
                ..
            } = ctx;

            let s3_client =
                s3_client.ok_or_else(|| anyhow::anyhow!("s3 client not initialized"))?;

            let uploader = match &config.user_config.uploader {
                None => {
                    tracing::warn!("Starting without archive uploader");
                    OptionalArchiveSubscriber::BlackHole
                }
                Some(config) => {
                    let uploader = ArchiveUploader::new(config.clone(), s3_client.clone())
                        .context("failed to create archive uploader")?;
                    uploader.upload_committed_archives(&storage).await?;
                    OptionalArchiveSubscriber::ArchiveUploader(uploader)
                }
            };
            let handler = ArchiveHandler::new(storage.clone(), uploader)?;

            let applier = ShardStateApplier::new(storage.clone(), rpc_states);

            let mut state_uploader = match &config.user_config.uploader {
                None => {
                    tracing::warn!("Starting without state uploader");
                    OptionalStateUploader::BlackHole
                }
                Some(config) => {
                    let uploader =
                        StateUploader::new(config.clone(), storage.clone(), s3_client.clone())
                            .context("failed to create state uploader")?;

                    OptionalStateUploader::StateUploader(uploader)
                }
            };
            state_uploader.run()?;

            let provider = tycho_core_bridge::standard_provider_chain(
                storage.clone(),
                node.blockchain_rpc_client.clone(),
                config.base.archive_block_provider.clone(),
                config.base.blockchain_block_provider.clone(),
            );
            let strider = BlockStrider::builder()
                .with_provider(provider)
                .with_state(strider_state)
                .with_block_subscriber((applier, handler, rpc_blocks, MetricsSubscriber))
                .build();

            Ok(strider)
        },
    )
}
