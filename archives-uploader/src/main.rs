use anyhow::Context;
use clap::Parser;
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveHandler, BlockProviderExt, MetricsSubscriber, ShardStateApplier,
};
use tycho_core::blockchain_rpc::NoopBroadcastListener;
use tycho_core::node::{CmdRunArgs, CmdRunStatus, NodeBase};
use tycho_rpc::NodeBaseInitRpc;
use tycho_util::cli::logger::{init_logger, set_abort_with_tracing};
use tycho_util::cli::metrics::init_metrics;
use tycho_util::cli::signal;

use crate::config::{NodeConfig, UserConfig};
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

type Config = NodeConfig<UserConfig>;

fn main() -> anyhow::Result<()> {
    let args = ExplorerArgs::parse();

    let node_args = match args.node.init_config_or_run::<Config>()? {
        CmdRunStatus::Run(args) => args,
        CmdRunStatus::ConfigCreated => return Ok(()),
    };

    let config: Config = node_args.load_config()?;

    init_logger(&config.logger_config, node_args.logger_config.clone())?;
    set_abort_with_tracing();

    let threads = config.threads;

    threads.init_all_and_run(async move {
        if let Some(metrics) = config.metrics.as_ref() {
            init_metrics(metrics)?;
        }

        let keys = node_args.load_keys()?;
        let global_config = node_args.load_global_config()?;
        let public_addr = config.base.resolve_public_ip().await?;

        let node = NodeBase::builder(&config.base, &global_config)
            .init_network(public_addr, &keys.as_secret())?
            .init_storage()
            .await?
            .init_blockchain_rpc(NoopBroadcastListener, NoopBroadcastListener)?
            .build()?;

        let init_block_id = node.init_ext(node_args.make_boot_args()).await?;
        node.update_validator_set_from_shard_state(&init_block_id)
            .await?;

        // Providers
        let s3_client = node
            .s3_client
            .clone()
            .context("s3 client not initialized")?;

        let archive_block_provider = ArchiveBlockProvider::new(
            s3_client.clone(),
            node.core_storage.clone(),
            config.base.archive_block_provider.clone(),
        );

        let storage_block_provider = node.build_storage_block_provider();

        let blockchain_block_provider = node
            .build_blockchain_block_provider()
            .with_fallback(archive_block_provider.clone());

        // Subscribers
        let (rpc_blocks, rpc_states) = node
            .init_simple_rpc_opt(&init_block_id, config.rpc.as_ref())
            .await?;

        let archive_uploader = match &config.user_config.uploader {
            None => {
                tracing::warn!("Starting without archive uploader");
                OptionalArchiveSubscriber::BlackHole
            }
            Some(c) => {
                let uploader = ArchiveUploader::new(c.clone(), s3_client.clone())
                    .context("failed to create archive uploader")?;
                uploader
                    .upload_committed_archives(&node.core_storage)
                    .await?;
                OptionalArchiveSubscriber::ArchiveUploader(uploader)
            }
        };
        let archive_handler = ArchiveHandler::new(node.core_storage.clone(), archive_uploader)?;

        let state_applier = ShardStateApplier::new(node.core_storage.clone(), rpc_states);

        let mut state_uploader = match &config.user_config.uploader {
            None => {
                tracing::warn!("Starting without state uploader");
                OptionalStateUploader::BlackHole
            }
            Some(config) => {
                let uploader =
                    StateUploader::new(config.clone(), node.core_storage.clone(), s3_client)
                        .context("failed to create state uploader")?;

                OptionalStateUploader::StateUploader(uploader)
            }
        };

        let block_strider = node.build_strider(
            archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
            (
                state_applier,
                archive_handler,
                rpc_blocks,
                node.validator_resolver().clone(),
                MetricsSubscriber,
            ),
        );

        // run the state uploader alongside the block strider
        signal::run_or_terminate(async move {
            tokio::select! {
                result = state_uploader.run() => result,
                result = block_strider.run() => result,
            }
        })
        .await
    })
}
