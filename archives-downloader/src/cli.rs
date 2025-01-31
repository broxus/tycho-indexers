use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use everscale_types::models::*;
use everscale_types::prelude::{Boc, CellBuilder};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::block_strider::{
    BlockProvider, BlockStrider, BlockSubscriber, BlockSubscriberExt, FileZerostateProvider,
    GcSubscriber, PersistentBlockStriderState, ZerostateProvider,
};
use tycho_core::global_config::{GlobalConfig, ZerostateId};
use tycho_storage::{BlockHandle, NewBlockMeta, Storage};
use tycho_util::cli::logger::init_logger;
use tycho_util::FastHashMap;

use crate::config::NodeConfig;

/// Run a Tycho node.
#[derive(Args, Clone)]
pub struct CmdRun {
    /// dump the template of the zero state config
    #[clap(
        short = 'i',
        long,
        conflicts_with_all = ["config", "global_config", "keys", "logger_config", "import_zerostate"]
    )]
    pub init_config: Option<PathBuf>,

    /// overwrite the existing config
    #[clap(short, long)]
    pub force: bool,

    /// path to the node config
    #[clap(long, required_unless_present = "init_config")]
    pub config: Option<PathBuf>,

    /// path to the global config
    #[clap(long, required_unless_present = "init_config")]
    pub global_config: Option<PathBuf>,

    /// path to the logger config
    #[clap(long)]
    pub logger_config: Option<PathBuf>,

    /// list of zerostate files to import
    #[clap(long)]
    pub import_zerostate: Option<Vec<PathBuf>>,
}

impl CmdRun {
    pub async fn create<C>(self, node_config: NodeConfig<C>) -> Result<Node<C>>
    where
        C: Clone,
    {
        init_logger(&node_config.logger_config, self.logger_config.clone())?;

        if let Some(metrics) = &node_config.metrics {
            tycho_util::cli::metrics::init_metrics(metrics)?;
        }

        let node = {
            let global_config = GlobalConfig::from_file(self.global_config.unwrap())
                .context("failed to load global config")?;

            Node::new(node_config, global_config).await?
        };

        Ok(node)
    }
}

pub struct Node<C> {
    zerostate: ZerostateId,

    storage: Storage,

    run_handle: Option<tokio::task::JoinHandle<()>>,

    _config: NodeConfig<C>,
}

impl<C> Node<C> {
    pub async fn new(node_config: NodeConfig<C>, global_config: GlobalConfig) -> Result<Node<C>>
    where
        C: Clone,
    {
        let config = node_config.clone();

        // Setup storage
        let storage = Storage::builder()
            .with_config(node_config.storage)
            .build()
            .await
            .context("failed to create storage")?;
        tracing::info!(
            root_dir = %storage.root().path().display(),
            "initialized storage"
        );

        let zerostate = global_config.zerostate;

        Ok(Self {
            zerostate,
            storage,
            _config: config,
            run_handle: None,
        })
    }

    pub async fn init(&self, import_zerostate: Option<Vec<PathBuf>>) -> Result<BlockId> {
        let last_mc_block_id = self
            .boot(import_zerostate)
            .await
            .context("failed to init node")?;

        tracing::info!(%last_mc_block_id, "node initialized");

        Ok(last_mc_block_id)
    }

    /// Initialize the node and return the last mc block id.
    async fn boot(&self, zerostates: Option<Vec<PathBuf>>) -> Result<BlockId> {
        let node_state = self.storage.node_state();

        let last_mc_block_id = match node_state.load_last_mc_block_id() {
            Some(block_id) => block_id,
            None => {
                let zerostates = zerostates.expect("zerostate should be present");
                let provider = FileZerostateProvider(zerostates);
                let (handle, _) = self.import_zerostates(provider).await?;
                *handle.id()
            }
        };

        tracing::info!(
            %last_mc_block_id,
            "boot finished"
        );

        Ok(last_mc_block_id)
    }

    pub async fn run<P, S>(&mut self, provider: P, subscriber: S) -> Result<()>
    where
        P: BlockProvider,
        S: BlockSubscriber,
    {
        let strider_state =
            PersistentBlockStriderState::new(self.zerostate.as_block_id(), self.storage.clone());

        let gc_subscriber = GcSubscriber::new(self.storage.clone());

        let block_strider = BlockStrider::builder()
            .with_provider(provider)
            .with_state(strider_state)
            .with_block_subscriber(subscriber.chain(gc_subscriber))
            .build();

        // Run block strider
        let handle = tokio::spawn(async move {
            tracing::info!("block strider started");
            if let Err(e) = block_strider.run().await {
                tracing::error!(%e, "block strider failed");
            }
            tracing::info!("block strider finished");
        });
        self.run_handle = Some(handle);

        Ok(())
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    #[allow(dead_code)]
    pub fn stop(&mut self) {
        if let Some(handle) = self.run_handle.take() {
            handle.abort();
        }
    }

    async fn import_zerostates<P>(&self, provider: P) -> Result<(BlockHandle, ShardStateStuff)>
    where
        P: ZerostateProvider,
    {
        tracing::info!("import zerostates");
        // Use a separate tracker for zerostates
        let tracker = MinRefMcStateTracker::default();

        // Read all zerostates
        let mut zerostates = FastHashMap::default();
        for loaded in provider.load_zerostates(&tracker) {
            let state = loaded?;
            if let Some(prev) = zerostates.insert(*state.block_id(), state) {
                anyhow::bail!("duplicate zerostate {}", prev.block_id());
            }
        }

        // Find the masterchain zerostate
        let zerostate_id = self.zerostate.as_block_id();
        let Some(masterchain_zerostate) = zerostates.remove(&zerostate_id) else {
            anyhow::bail!("missing mc zerostate for {zerostate_id}");
        };

        // Prepare the list of zerostates to import
        let mut to_import = vec![masterchain_zerostate.clone()];

        let global_id = masterchain_zerostate.state().global_id;
        let gen_utime = masterchain_zerostate.state().gen_utime;

        for entry in masterchain_zerostate.shards()?.iter() {
            let (shard_ident, descr) = entry.context("invalid mc zerostate")?;
            anyhow::ensure!(descr.seqno == 0, "invalid shard description {shard_ident}");

            let block_id = BlockId {
                shard: shard_ident,
                seqno: 0,
                root_hash: descr.root_hash,
                file_hash: descr.file_hash,
            };

            let state = match zerostates.remove(&block_id) {
                Some(existing) => {
                    tracing::debug!(block_id = %block_id, "using custom zerostate");
                    existing
                }
                None => {
                    tracing::debug!(block_id = %block_id, "creating default zerostate");
                    let state = make_shard_state(&tracker, global_id, shard_ident, gen_utime)
                        .context("failed to create shard zerostate")?;

                    tracing::info!(state = ?state.block_id(), block = ?block_id);

                    anyhow::ensure!(
                        state.block_id() == &block_id,
                        "custom zerostate must be provided for {shard_ident}",
                    );

                    state
                }
            };

            to_import.push(state);
        }

        anyhow::ensure!(
            zerostates.is_empty(),
            "unused zerostates left: {}",
            zerostates.len()
        );

        // Import all zerostates
        let handle_storage = self.storage.block_handle_storage();
        let state_storage = self.storage.shard_state_storage();
        let persistent_states = self.storage.persistent_state_storage();

        for state in to_import {
            let (handle, status) =
                handle_storage.create_or_load_handle(state.block_id(), NewBlockMeta {
                    is_key_block: state.block_id().is_masterchain(),
                    gen_utime,
                    ref_by_mc_seqno: 0,
                });

            let stored = state_storage
                .store_state(&handle, &state, Default::default())
                .await
                .with_context(|| {
                    format!("failed to import zerostate for {}", state.block_id().shard)
                })?;

            tracing::debug!(
                block_id = %state.block_id(),
                handle_status = ?status,
                stored,
                "importing zerostate"
            );

            persistent_states
                .store_shard_state(0, &handle, state.ref_mc_state_handle().clone())
                .await?;
        }

        tracing::info!("imported zerostates");

        let state = state_storage.load_state(&zerostate_id).await?;
        let handle = handle_storage
            .load_handle(&zerostate_id)
            .expect("shouldn't happen");

        Ok((handle, state))
    }
}

fn make_shard_state(
    tracker: &MinRefMcStateTracker,
    global_id: i32,
    shard_ident: ShardIdent,
    now: u32,
) -> Result<ShardStateStuff> {
    let state = ShardStateUnsplit {
        global_id,
        shard_ident,
        gen_utime: now,
        min_ref_mc_seqno: u32::MAX,
        ..Default::default()
    };

    let root = CellBuilder::build_from(&state)?;
    let root_hash = *root.repr_hash();
    let file_hash = Boc::file_hash_blake(Boc::encode(&root));

    let block_id = BlockId {
        shard: state.shard_ident,
        seqno: state.seqno,
        root_hash,
        file_hash,
    };

    ShardStateStuff::from_root(&block_id, root, tracker)
}
