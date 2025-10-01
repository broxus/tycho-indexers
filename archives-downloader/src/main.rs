use anyhow::Context;
use clap::Parser;
use tycho_core::block_strider::{ColdBootType, PsSubscriber, ShardStateApplier};
use tycho_util::cli::logger::init_logger;
use tycho_util::cli::metrics::spawn_allocator_metrics_loop;

use crate::config::UserConfig;
use crate::provider::{ArchiveBlockProvider, ArchiveBlockProviderConfig};

mod config;
mod provider;

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

    let init_block_id = node.init(ColdBootType::Genesis, import_zerostate).await?;

    let archive_block_provider = ArchiveBlockProvider::new(
        node.storage().clone(),
        ArchiveBlockProviderConfig {
            bucket_name: config.user_config.bucket_name,
            s3_provider: config.user_config.s3_provider,
            max_archive_to_memory_size: config.archive_block_provider.max_archive_to_memory_size,
        },
    )?;

    let rpc_state = node
        .create_rpc(&init_block_id)
        .await?
        .map(|x| x.split())
        .unzip();

    let state_applier = {
        let storage = node.storage();
        let ps_subscriber = PsSubscriber::new(storage.clone());

        ShardStateApplier::new(storage.clone(), (rpc_state.1, ps_subscriber))
    };

    node.run(archive_block_provider, (state_applier, rpc_state.0))
        .await?;

    Ok(tokio::signal::ctrl_c().await?)
}
