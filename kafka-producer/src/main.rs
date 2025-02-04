use anyhow::Context;
use clap::Parser;
use tikv_jemalloc_ctl::{epoch, stats};
use tycho_core::block_strider::{
    ArchiveBlockProvider, BlockProviderExt, BlockchainBlockProvider, PsSubscriber,
    ShardStateApplier, StorageBlockProvider,
};

use crate::config::UserConfig;
use crate::subscriber::{KafkaProducer, OptionalStateSubscriber};

mod config;
mod subscriber;

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

    let import_zerostate = args.node.import_zerostate.clone();

    let writer = match &config.user_config.kafka {
        None => {
            tracing::warn!("Starting without kafka producer");
            OptionalStateSubscriber::Blackhole
        }
        Some(c) => {
            let producer = KafkaProducer::new(c.clone())
                .await
                .context("failed to create kafka subscriber")?;
            OptionalStateSubscriber::KafkaProducer(producer)
        }
    };

    if config.metrics.is_some() {
        spawn_allocator_metrics_loop();
    }
    spawn_allocator_metrics_loop();

    let mut node = args.node.create(config.clone()).await?;
    let init_block_id = node.init(import_zerostate, false).await?;
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

    let state_applier = {
        let storage = node.storage();
        let ps_subscriber = PsSubscriber::new(storage.clone());

        ShardStateApplier::new(storage.clone(), (rpc_state.1, writer, ps_subscriber))
    };

    node.run(
        archive_block_provider.chain((blockchain_block_provider, storage_block_provider)),
        (state_applier, rpc_state.0),
    )
    .await?;

    Ok(tokio::signal::ctrl_c().await?)
}

macro_rules! set_metrics {
    ($($metric_name:expr => $metric_value:expr),* $(,)?) => {
        $(
            metrics::gauge!($metric_name).set($metric_value as f64);
        )*
    };
}

pub fn spawn_allocator_metrics_loop() {
    tokio::spawn(async move {
        loop {
            let s = match fetch_stats() {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("failed to fetch jemalloc stats: {e}");
                    return;
                }
            };

            set_metrics!(
                "jemalloc_allocated_bytes" => s.allocated,
                "jemalloc_active_bytes" => s.active,
                "jemalloc_metadata_bytes" => s.metadata,
                "jemalloc_resident_bytes" => s.resident,
                "jemalloc_mapped_bytes" => s.mapped,
                "jemalloc_retained_bytes" => s.retained,
                "jemalloc_dirty_bytes" => s.dirty,
                "jemalloc_fragmentation_bytes" => s.fragmentation,
            );
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

pub struct JemallocStats {
    pub allocated: u64,
    pub active: u64,
    pub metadata: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
    pub dirty: u64,
    pub fragmentation: u64,
}

fn fetch_stats() -> anyhow::Result<JemallocStats, tikv_jemalloc_ctl::Error> {
    // Stats are cached. Need to advance epoch to refresh.
    epoch::advance()?;

    Ok(JemallocStats {
        allocated: stats::allocated::read()? as u64,
        active: stats::active::read()? as u64,
        metadata: stats::metadata::read()? as u64,
        resident: stats::resident::read()? as u64,
        mapped: stats::mapped::read()? as u64,
        retained: stats::retained::read()? as u64,
        dirty: (stats::resident::read()?
            .saturating_sub(stats::active::read()?)
            .saturating_sub(stats::metadata::read()?)) as u64,
        fragmentation: (stats::active::read()?.saturating_sub(stats::allocated::read()?)) as u64,
    })
}
