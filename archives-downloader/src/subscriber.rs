use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::block_strider::{BlockSaver, BlockSubscriber, BlockSubscriberContext};
use tycho_core::storage::{CoreStorage, StoreStateHint};
use tycho_types::cell::Cell;
use tycho_types::models::{BlockId, ShardIdent, ShardStateUnsplit};
use tycho_util::FastHashMap;
use tycho_util::sync::rayon_run;

pub struct PoorStateApplier {
    storage: CoreStorage,
    block_saver: BlockSaver,
    states: Arc<Mutex<FastHashMap<ShardIdent, Cell>>>,
}

impl PoorStateApplier {
    pub async fn new(mc_block_id: &BlockId, storage: &CoreStorage) -> Result<Self> {
        let shard_states = storage.shard_state_storage();

        let mc_state = shard_states
            .load_state(mc_block_id.seqno, mc_block_id)
            .await?;

        let mut states = FastHashMap::default();
        states.insert(mc_block_id.shard, mc_state.root_cell().clone());

        Ok(PoorStateApplier {
            storage: storage.clone(),
            block_saver: BlockSaver::new(storage.clone()),
            states: Arc::new(Mutex::new(states)),
        })
    }

    pub async fn apply_merkle_update(&self, cx: &BlockSubscriberContext) -> Result<()> {
        tracing::info!(
            block_id = ?cx.block.id().as_short_id(),
            "apply merkle update"
        );

        let shard = cx.block.id().shard;

        let prev_root = if let Some(prev_root) = self.states.lock().get(&shard) {
            prev_root.clone()
        } else {
            let prev_block = cx.block.construct_prev_id()?.0;

            let handle = self
                .storage
                .block_handle_storage()
                .load_handle(&prev_block)
                .unwrap();

            let prev_root = self
                .storage
                .shard_state_storage()
                .load_state(handle.meta().ref_by_mc_seqno(), &prev_block)
                .await?;

            prev_root.root_cell().clone()
        };

        let update = cx
            .block
            .as_ref()
            .load_state_update()
            .context("Failed to load state update")?;

        let mut new_state = rayon_run(move || update.par_apply(&prev_root, &Default::default()))
            .await
            .context("Failed to apply state update")?;

        // Save key blocks with states
        if cx.is_key_block || cx.is_top_block || cx.mc_block_id.seqno % 50 == 0 {
            if !cx.block.id().is_masterchain() {
                tracing::info!(block_id = ?cx.block.id().as_short_id(), "save shard block");
            }

            let handle = self.block_saver.save_block(cx).await?;

            let state_storage = self.storage.shard_state_storage();

            let shard_state = new_state.parse::<Box<ShardStateUnsplit>>()?;
            let min_ref_mc_state = state_storage.min_ref_mc_state().insert(&shard_state);

            let shard_state_stuff = ShardStateStuff::from_state_and_root(
                cx.block.id(),
                shard_state,
                new_state,
                min_ref_mc_state,
            )
            .context("Failed to create new state")?;

            state_storage
                .store_state(&handle, &shard_state_stuff, StoreStateHint {
                    block_data_size: Some(cx.block.data_size()),
                })
                .await
                .context("Failed to store new state")?;

            // Pure state
            new_state = self
                .storage
                .shard_state_storage()
                .load_state(cx.mc_block_id.seqno, cx.block.id())
                .await?
                .root_cell()
                .clone();
        }

        // Update cache
        self.states.lock().insert(shard, new_state);

        Ok(())
    }
}

impl BlockSubscriber for PoorStateApplier {
    type Prepared = ();

    type PrepareBlockFut<'a> = BoxFuture<'a, Result<Self::Prepared>>;
    type HandleBlockFut<'a> = futures_util::future::Ready<Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        Box::pin(self.apply_merkle_update(cx))
    }

    fn handle_block<'a>(
        &'a self,
        _: &'a BlockSubscriberContext,
        _: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        futures_util::future::ready(Ok(()))
    }
}
