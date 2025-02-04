use anyhow::Context;
use clap::Parser;
use tikv_jemalloc_ctl::{epoch, stats};
use tycho_core::block_strider::{PsSubscriber, ShardStateApplier};

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

    let import_zerostate = args.node.import_zerostate.clone();

    if config.metrics.is_some() {
        spawn_allocator_metrics_loop();
    }
    spawn_allocator_metrics_loop();

    let mut node = args.node.create(config.clone()).await?;

    node.init(import_zerostate, true).await?;

    let archive_block_provider =
        ArchiveBlockProvider::new(node.storage().clone(), ArchiveBlockProviderConfig {
            bucket_name: config.user_config.bucket_name,
            s3_provider: config.user_config.s3_provider,
            max_archive_to_memory_size: config.archive_block_provider.max_archive_to_memory_size,
        })?;

    let state_applier = {
        let storage = node.storage();
        let ps_subscriber = PsSubscriber::new(storage.clone());

        ShardStateApplier::new(storage.clone(), ps_subscriber)
    };

    node.run(archive_block_provider, state_applier).await?;

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
