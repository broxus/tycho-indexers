use anyhow::Context;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{
    ArchiveBlockProvider, MetricsSubscriber, NoopSubscriber, ShardStateApplier,
};
use tycho_core::blockchain_rpc::NoopBroadcastListener;
use tycho_core::node::{CmdRunArgs, CmdRunStatus, NodeBase, NodeBaseConfig};
use tycho_rpc::RpcConfig;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::{LoggerConfig, init_logger, set_abort_with_tracing};
use tycho_util::cli::metrics::{MetricsConfig, init_metrics};
use tycho_util::cli::signal;
use tycho_util::config::PartialConfig;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct ExplorerArgs {
    #[clap(flatten)]
    node: CmdRunArgs,
}

type Config = NodeConfig<UserConfig>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct NodeConfig<T> {
    #[serde(flatten)]
    base: NodeBaseConfig,
    rpc: Option<RpcConfig>,
    metrics: Option<MetricsConfig>,
    threads: ThreadPoolConfig,
    logger_config: LoggerConfig,
    #[serde(flatten)]
    user_config: T,
}

impl<T> Default for NodeConfig<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            base: Default::default(),
            rpc: Some(Default::default()),
            metrics: Some(Default::default()),
            threads: Default::default(),
            logger_config: Default::default(),
            user_config: Default::default(),
        }
    }
}

impl<T> PartialConfig for NodeConfig<T>
where
    T: Serialize,
{
    type Partial = Self;

    fn into_partial(self) -> Self::Partial {
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UserConfig {}

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

    threads.init_all_and_run(signal::run_or_terminate(async move {
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
            s3_client,
            node.core_storage.clone(),
            config.base.archive_block_provider.clone(),
        );

        // Subscribers
        let state_applier = ShardStateApplier::new(node.core_storage.clone(), NoopSubscriber);

        let block_strider =
            node.build_strider(archive_block_provider, (state_applier, MetricsSubscriber));

        block_strider.run().await
    }))
}
