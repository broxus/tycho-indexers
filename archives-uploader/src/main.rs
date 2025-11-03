use anyhow::Context;
use clap::Parser;
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveHandler, BlockProviderExt, BlockchainBlockProvider, ColdBootType,
    PsSubscriber, ShardStateApplier, StorageBlockProvider,
};
use tycho_util::cli::logger::init_logger;
use tycho_util::cli::metrics::spawn_allocator_metrics_loop;

use crate::config::UserConfig;
use crate::subscribers::{
    ArchiveUploader, OptionalArchiveSubscriber, OptionalStateSubscriber, StateUploader,
};

mod config;
mod subscribers;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct ExplorerArgs {
    #[clap(flatten)]
    node: tycho_light_node::CmdRun,
}

type Config = tycho_light_node::NodeConfig<UserConfig>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;
        let backtrace = std::backtrace::Backtrace::capture();

        tracing::error!("{info}\n{backtrace}");
        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();
        std::process::exit(1);
    }));

    let args = ExplorerArgs::parse();

    let config: Config =
        tycho_light_node::NodeConfig::from_file(args.node.config.as_ref().context("no config")?)?;

    init_logger(&config.logger_config, args.node.logger_config.clone())?;

    let import_zerostate = args.node.import_zerostate.clone();

    if config.metrics.is_some() {
        spawn_allocator_metrics_loop();
    }

    let mut node = args.node.create(config.clone()).await?;

    let init_block_id = {
        let state_handler = match &config.user_config.s3_state_uploader {
            None => {
                tracing::warn!("Starting without state uploader for cold boot");
                OptionalStateSubscriber::BlackHole
            }
            Some(c) => {
                let uploader = StateUploader::new(c.clone())
                    .context("failed to create state uploader for cold boot")?;
                OptionalStateSubscriber::StateUploader(uploader)
            }
        };

        node.init(
            ColdBootType::LatestPersistent,
            import_zerostate,
            Some(Box::new(state_handler)),
        )
        .await?
    };
    node.update_validator_set(&init_block_id).await?;

    // Providers
    let archive_block_provider = ArchiveBlockProvider::new(
        node.blockchain_rpc_client().clone(),
        node.storage().clone(),
        config.archive_block_provider.clone(),
    );

    let storage_block_provider = StorageBlockProvider::new(node.storage().clone());

    let blockchain_block_provider = BlockchainBlockProvider::new(
        node.blockchain_rpc_client().clone(),
        node.storage().clone(),
        config.blockchain_block_provider.clone(),
    )
    .with_fallback(archive_block_provider.clone());

    // Subscribers
    let rpc_state = node
        .create_rpc(&init_block_id)
        .await?
        .map(|x| x.split())
        .unzip();

    let archive_uploader = match &config.user_config.s3_archive_uploader {
        None => {
            tracing::warn!("Starting without archive uploader");
            OptionalArchiveSubscriber::BlackHole
        }
        Some(c) => {
            let uploader =
                ArchiveUploader::new(c.clone()).context("failed to create archive uploader")?;
            uploader.upload_committed_archives(node.storage()).await?;
            OptionalArchiveSubscriber::ArchiveUploader(uploader)
        }
    };

    let state_uploader = match &config.user_config.s3_state_uploader {
        None => {
            tracing::warn!("Starting without state uploader");
            OptionalStateSubscriber::BlackHole
        }
        Some(c) => {
            let uploader =
                StateUploader::new(c.clone()).context("failed to create state uploader")?;
            uploader.finish_last_state(node.storage()).await?;
            OptionalStateSubscriber::StateUploader(uploader)
        }
    };

    let state_applier = {
        let storage = node.storage();
        let ps_subscriber =
            PsSubscriber::with_completion_subscriber(storage.clone(), state_uploader);

        ShardStateApplier::new(storage.clone(), (rpc_state.1, ps_subscriber))
    };

    let archive_handler = ArchiveHandler::new(node.storage().clone(), archive_uploader)?;

    node.run(
        archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
        (state_applier, archive_handler, rpc_state.0),
    )
    .await?;

    Ok(tokio::signal::ctrl_c().await?)
}
