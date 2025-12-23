use std::future::Future;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, BlockProvider, BlockProviderExt,
    BlockStrider, BlockStriderState, BlockSubscriber, BlockchainBlockProvider,
    BlockchainBlockProviderConfig, PersistentBlockStriderState, StorageBlockProvider,
};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_core::node::{CmdRunArgs, LightNodeConfig, LightNodeContext, NodeBase, NodeBaseConfig};
#[cfg(feature = "s3")]
pub use tycho_core::s3::S3Client;
use tycho_core::storage::CoreStorage;
use tycho_rpc::{NodeBaseInitRpc, RpcBlockSubscriber, RpcConfig, RpcStateSubscriber};
use tycho_types::models::BlockId;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::LoggerConfig;
use tycho_util::cli::metrics::{MetricsConfig, spawn_allocator_metrics_loop};
use tycho_util::config::PartialConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig<T> {
    #[serde(flatten)]
    pub base: NodeBaseConfig,

    pub rpc: Option<RpcConfig>,

    pub metrics: Option<MetricsConfig>,

    pub threads: ThreadPoolConfig,

    pub logger_config: LoggerConfig,

    #[serde(flatten)]
    pub user_config: T,
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
    T: Default + Serialize + for<'de> Deserialize<'de>,
{
    type Partial = Self;

    fn into_partial(self) -> Self::Partial {
        self
    }
}

impl<T> LightNodeConfig for NodeConfig<T>
where
    T: Default + Serialize + for<'de> Deserialize<'de>,
{
    fn base(&self) -> &NodeBaseConfig {
        &self.base
    }

    fn threads(&self) -> &ThreadPoolConfig {
        &self.threads
    }

    fn metrics(&self) -> Option<&MetricsConfig> {
        self.metrics.as_ref()
    }

    fn logger(&self) -> Option<&LoggerConfig> {
        Some(&self.logger_config)
    }
}

pub struct RunContext<T> {
    pub node: NodeBase,
    pub config: NodeConfig<T>,
    pub init_block_id: BlockId,
    pub storage: CoreStorage,
    pub strider_state: PersistentBlockStriderState,
    pub rpc_blocks: Option<RpcBlockSubscriber>,
    pub rpc_states: Option<RpcStateSubscriber>,
    #[cfg(feature = "s3")]
    pub s3_client: Option<S3Client>,
}

/// Args:
/// BlockStrider returned by `logic` closure will be run on tokio, with unix signals handling etc
pub fn run_node<T, F, Fut, S, P, B>(args: CmdRunArgs, init_rpc: bool, logic: F) -> Result<()>
where
    T: Default + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    F: FnOnce(RunContext<T>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<BlockStrider<S, P, B>>> + Send + 'static,
    S: BlockStriderState,
    P: BlockProvider,
    B: BlockSubscriber,
{
    // CmdRunArgs::init_config_or_run_light_node installs its own panic hook.
    args.init_config_or_run_light_node::<_, _, NodeConfig<T>>(move |ctx| async move {
        let LightNodeContext {
            node,
            boot_args,
            config,
            ..
        } = ctx;

        if config.metrics.is_some() {
            spawn_allocator_metrics_loop();
        }

        let init_block_id = node.init_ext(boot_args).await?;
        node.update_validator_set_from_shard_state(&init_block_id)
            .await?;

        let storage = node.core_storage.clone();
        let strider_state = PersistentBlockStriderState::new(
            node.global_config.zerostate.as_block_id(),
            storage.clone(),
        );

        let (rpc_blocks, rpc_states) = if init_rpc {
            node.init_simple_rpc_opt(&init_block_id, config.rpc.as_ref())
                .await?
        } else {
            (None, None)
        };

        #[cfg(feature = "s3")]
        let s3_client = node.s3_client.clone();

        let strider = logic(RunContext {
            node,
            config,
            init_block_id,
            storage,
            strider_state,
            rpc_blocks,
            rpc_states,
            #[cfg(feature = "s3")]
            s3_client,
        })
        .await?;

        strider.run().await.context("block strider failed")?;

        tracing::info!("Block strider finished successfully");
        Ok(())
    })
}

pub fn standard_provider_chain(
    storage: CoreStorage,
    client: BlockchainRpcClient,
    archive_config: ArchiveBlockProviderConfig,
    blockchain_config: BlockchainBlockProviderConfig,
) -> impl BlockProvider {
    let archive = ArchiveBlockProvider::new(client.clone(), storage.clone(), archive_config);
    let storage_blocks = StorageBlockProvider::new(storage.clone());
    let blockchain = BlockchainBlockProvider::new(client, storage.clone(), blockchain_config)
        .with_fallback(archive.clone());

    archive.chain((blockchain, storage_blocks))
}
