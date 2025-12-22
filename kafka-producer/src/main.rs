use anyhow::{Context, Result};
use clap::Parser;
use tycho_core::block_strider::{BlockStrider, MetricsSubscriber, ShardStateApplier};
use tycho_core::node::CmdRunArgs;

use crate::config::UserConfig;
use crate::subscriber::{KafkaProducer, OptionalStateSubscriber};

mod config;
mod subscriber;

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
                ..
            } = ctx;

            let writer = match &config.user_config.kafka {
                None => {
                    tracing::warn!("Starting without kafka producer");
                    OptionalStateSubscriber::Blackhole
                }
                Some(config) => {
                    let producer = KafkaProducer::new(config.clone())
                        .await
                        .context("failed to create kafka subscriber")?;
                    OptionalStateSubscriber::KafkaProducer(producer)
                }
            };

            let applier = ShardStateApplier::new(storage.clone(), rpc_states);

            let provider = tycho_core_bridge::standard_provider_chain(
                storage.clone(),
                node.blockchain_rpc_client.clone(),
                config.base.archive_block_provider.clone(),
                config.base.blockchain_block_provider.clone(),
            );
            let strider = BlockStrider::builder()
                .with_provider(provider)
                .with_state(strider_state)
                .with_block_subscriber((writer, applier, rpc_blocks, MetricsSubscriber))
                .build();

            Ok(strider)
        },
    )
}
