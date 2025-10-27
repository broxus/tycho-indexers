use anyhow::Context;
use clap::Parser;
use tycho_core::block_strider::{
    ArchiveBlockProvider, ColdBootType, PsSubscriber, ShardStateApplier,
};
use tycho_util::cli::logger::init_logger;
use tycho_util::cli::metrics::spawn_allocator_metrics_loop;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct ExplorerArgs {
    #[clap(flatten)]
    node: tycho_light_node::CmdRun,
}

type Config = tycho_light_node::NodeConfig<()>;

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
        node.init(ColdBootType::LatestPersistent, import_zerostate, false)
            .await?
    };
    node.update_validator_set(&init_block_id).await?;

    // Providers
    let s3_client = node
        .s3_client()
        .ok_or(anyhow::anyhow!("s3 client not initialized"))?;

    let archive_block_provider = ArchiveBlockProvider::new(
        s3_client.clone(),
        node.storage().clone(),
        config.archive_block_provider.clone(),
    );

    // Subscribers
    let state_applier = {
        let storage = node.storage();
        let ps_subscriber = PsSubscriber::new(storage.clone());
        ShardStateApplier::new(storage.clone(), ps_subscriber)
    };

    // Run node
    node.run(archive_block_provider, state_applier).await?;

    Ok(tokio::signal::ctrl_c().await?)
}
