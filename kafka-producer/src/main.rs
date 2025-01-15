use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use tikv_jemalloc_ctl::{epoch, stats};

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
    let writer = match &config.user_config.kafka {
        None => {
            tracing::warn!("Starting without kafka producer");
            OptionalStateSubscriber::Blackhole
        }
        Some(c) => {
            let producer =
                KafkaProducer::new(c.clone()).context("failed to create kafka subsbriber")?;
            OptionalStateSubscriber::KafkaProducer(producer)
        }
    };

    if let Some(metrics) = &config.metrics {
        init_metrics(metrics.listen_addr)?;
    }
    spawn_allocator_metrics_loop();

    args.node.run(config, writer).await?;

    Ok(tokio::signal::ctrl_c().await?)
}

fn init_metrics(config: SocketAddr) -> anyhow::Result<()> {
    use metrics_exporter_prometheus::Matcher;
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.75, 1.0, 2.5, 5.0,
        7.5, 10.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0,
    ];

    const EXPONENTIAL_LONG_SECONDS: &[f64] = &[
        0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
        14400.0, 28800.0, 43200.0, 86400.0,
    ];

    const EXPONENTIAL_THREADS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_time".to_string()), EXPONENTIAL_SECONDS)?
        .set_buckets_for_metric(Matcher::Suffix("_threads".to_string()), EXPONENTIAL_THREADS)?
        .set_buckets_for_metric(
            Matcher::Suffix("_time_long".to_string()),
            EXPONENTIAL_LONG_SECONDS,
        )?
        .with_http_listener(config)
        .install()?;

    Ok(())
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
