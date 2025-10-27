use std::collections::{BTreeMap, btree_map};
use std::io::{Seek, Write};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_block_util::archive::{Archive, ArchiveVerifier};
use tycho_block_util::block::{BlockIdRelation, BlockStuffAug};
use tycho_core::block_strider::{BlockProvider, CheckProof, OptionalBlockStuff, ProofChecker};
use tycho_core::storage::CoreStorage;
use tycho_storage::fs::MappedFile;
use tycho_types::models::BlockId;
use tycho_util::compression::ZstdDecompress;

use crate::config::ArchiveProviderConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveBlockProviderConfig {
    pub bucket_name: String,
    pub s3_provider: ArchiveProviderConfig,
    pub max_archive_to_memory_size: ByteSize,
}

impl Default for ArchiveBlockProviderConfig {
    fn default() -> Self {
        Self {
            bucket_name: Default::default(),
            s3_provider: Default::default(),
            max_archive_to_memory_size: ByteSize::mb(100),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ArchiveBlockProvider {
    inner: Arc<Inner>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self.archive_ids_task.take() {
            handle.abort();
        }
    }
}

impl ArchiveBlockProvider {
    pub async fn new(storage: CoreStorage, config: ArchiveBlockProviderConfig) -> Result<Self> {
        let proof_checker = ProofChecker::new(storage.clone());

        let s3_client: Arc<DynObjectStore> = match &config.s3_provider {
            ArchiveProviderConfig::Aws {
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
            } => Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(&config.bucket_name)
                    .with_endpoint(endpoint)
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key)
                    .with_client_options(
                        object_store::ClientOptions::new().with_allow_http(*allow_http),
                    )
                    .build()?,
            ),
            ArchiveProviderConfig::Gcs { credentials_path } => Arc::new(
                GoogleCloudStorageBuilder::new()
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_connect_timeout_disabled()
                            .with_timeout_disabled(),
                    )
                    .with_bucket_name(&config.bucket_name)
                    .with_application_credentials(credentials_path)
                    .build()?,
            ),
        };

        let res = Inner {
            config,
            storage,
            s3_client,
            proof_checker,
            archive_ids_task: None,
            archive_ids: Default::default(),
            known_archives: Mutex::new(Default::default()),
        };

        res.make_downloader().preload_archive_ids().await;

        // res.archive_ids_task =
        //     Some(tokio::spawn(res.make_downloader().update_archive_ids_task()).abort_handle());

        Ok(Self {
            inner: Arc::new(res),
        })
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        tracing::info!(?block_id, "get_next_block_impl");

        let this = self.inner.as_ref();

        let next_mc_seqno = block_id.seqno + 1;

        loop {
            let Some((archive_key, info)) = this.get_archive(next_mc_seqno).await else {
                tracing::info!(mc_seqno = next_mc_seqno, "archive block provider finished");
                break None;
            };

            let Some(block_id) = info.archive.mc_block_ids.get(&next_mc_seqno) else {
                tracing::error!(
                    archive_id = archive_key,
                    "received archive does not contain mc block with seqno {next_mc_seqno}"
                );
                this.remove_archive_if_same(archive_key, &info);
                continue;
            };

            match self
                .checked_get_entry_by_id(&info.archive, block_id, block_id)
                .await
            {
                Ok(block) => return Some(Ok(block.clone())),
                Err(e) => {
                    tracing::error!(archive_key, %block_id, "invalid archive entry: {e}");
                    this.remove_archive_if_same(archive_key, &info);
                }
            }
        }
    }

    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

        let block_id = block_id_relation.block_id;
        let mc_block_id = block_id_relation.mc_block_id;

        loop {
            let Some((archive_key, info)) = this.get_archive(mc_block_id.seqno).await else {
                tracing::warn!("shard block is too new for archives");

                // NOTE: This is a strange situation, but if we wait a bit it might go away.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            match self
                .checked_get_entry_by_id(&info.archive, &mc_block_id, &block_id)
                .await
            {
                Ok(block) => return Some(Ok(block.clone())),
                Err(e) => {
                    tracing::error!(archive_key, %block_id, %mc_block_id, "invalid archive entry: {e}");
                    this.remove_archive_if_same(archive_key, &info);
                }
            }
        }
    }

    async fn checked_get_entry_by_id(
        &self,
        archive: &Arc<Archive>,
        mc_block_id: &BlockId,
        block_id: &BlockId,
    ) -> Result<BlockStuffAug> {
        let (block, ref proof, ref queue_diff) = match archive.get_entry_by_id(block_id).await {
            Ok(entry) => entry,
            Err(e) => anyhow::bail!("archive is corrupted: {e:?}"),
        };

        self.inner
            .proof_checker
            .check_proof(CheckProof {
                mc_block_id,
                block: &block,
                proof,
                queue_diff,
                store_on_success: true,
            })
            .await?;

        Ok(block)
    }
}

struct Inner {
    storage: CoreStorage,

    s3_client: Arc<DynObjectStore>,

    proof_checker: ProofChecker,

    archive_ids: Arc<RwLock<BTreeMap<u32, usize>>>,

    known_archives: Mutex<ArchivesMap>,

    config: ArchiveBlockProviderConfig,

    archive_ids_task: Option<AbortHandle>,
}

impl Inner {
    async fn get_archive(&self, mc_seqno: u32) -> Option<(u32, ArchiveInfo)> {
        loop {
            let mut pending = 'pending: {
                let mut guard = self.known_archives.lock();

                // Search for the downloaded archive or for and existing downloader task.
                for (archive_key, value) in guard.iter() {
                    match value {
                        ArchiveSlot::Downloaded(info) => {
                            if info.archive.mc_block_ids.contains_key(&mc_seqno) {
                                return Some((*archive_key, info.clone()));
                            }
                        }
                        ArchiveSlot::Pending(task) => break 'pending task.clone(),
                    }
                }

                // Start downloading otherwise
                let task = self.make_downloader().spawn(mc_seqno);
                guard.insert(mc_seqno, ArchiveSlot::Pending(task.clone()));

                task
            };

            // Wait until the pending task is finished or cancelled
            let mut res = None;
            let mut finished = false;
            loop {
                match &*pending.rx.borrow_and_update() {
                    ArchiveTaskState::None => {}
                    ArchiveTaskState::Finished(archive) => {
                        res = archive.clone();
                        finished = true;
                        break;
                    }
                    ArchiveTaskState::Cancelled => break,
                }
                if pending.rx.changed().await.is_err() {
                    break;
                }
            }

            // Replace pending with downloaded
            match self.known_archives.lock().entry(pending.archive_key) {
                btree_map::Entry::Vacant(_) => {
                    // Do nothing if the entry was already removed.
                }
                btree_map::Entry::Occupied(mut entry) => match &res {
                    None => {
                        // Task was either cancelled or received `TooNew` so no archive received.
                        entry.remove();
                    }
                    Some(info) => {
                        // Task was finished with a non-empty result so store it.
                        entry.insert(ArchiveSlot::Downloaded(info.clone()));
                    }
                },
            }

            if finished {
                return res.map(|info| (pending.archive_key, info));
            }

            tracing::warn!(mc_seqno, "archive task cancelled while in use");
            // Avoid spinloop just in case.
            tokio::task::yield_now().await;
        }
    }

    fn remove_archive_if_same(&self, archive_key: u32, prev: &ArchiveInfo) -> bool {
        match self.known_archives.lock().entry(archive_key) {
            btree_map::Entry::Vacant(_) => false,
            btree_map::Entry::Occupied(entry) => {
                if matches!(
                    entry.get(),
                    ArchiveSlot::Downloaded(info)
                    if Arc::ptr_eq(&info.archive, &prev.archive)
                ) {
                    entry.remove();
                    true
                } else {
                    false
                }
            }
        }
    }

    fn make_downloader(&self) -> ArchiveDownloader {
        ArchiveDownloader {
            archive_ids: self.archive_ids.clone(),
            s3_client: self.s3_client.clone(),
            storage: self.storage.clone(),
            memory_threshold: self.config.max_archive_to_memory_size,
        }
    }

    fn clear_outdated_archives(&self, bound: u32) {
        // TODO: Move into archive stuff
        const MAX_MC_PER_ARCHIVE: u32 = 100;

        let mut entries_remaining = 0usize;
        let mut entries_removed = 0usize;

        let mut guard = self.known_archives.lock();
        guard.retain(|_, archive| {
            let retain;
            match archive {
                ArchiveSlot::Downloaded(info) => match info.archive.mc_block_ids.last_key_value() {
                    None => retain = false,
                    Some((last_mc_seqno, _)) => retain = *last_mc_seqno >= bound,
                },
                ArchiveSlot::Pending(task) => {
                    retain = task.archive_key.saturating_add(MAX_MC_PER_ARCHIVE) >= bound;
                    if !retain {
                        task.abort_handle.abort();
                    }
                }
            };

            entries_remaining += retain as usize;
            entries_removed += !retain as usize;
            retain
        });
        drop(guard);

        tracing::debug!(
            entries_remaining,
            entries_removed,
            bound,
            "removed known archives"
        );
    }
}

type ArchivesMap = BTreeMap<u32, ArchiveSlot>;

enum ArchiveSlot {
    Downloaded(ArchiveInfo),
    Pending(ArchiveTask),
}

#[derive(Clone)]
struct ArchiveInfo {
    archive: Arc<Archive>,
}

struct ArchiveDownloader {
    archive_ids: Arc<RwLock<BTreeMap<u32, usize>>>,
    s3_client: Arc<DynObjectStore>,
    storage: CoreStorage,
    memory_threshold: ByteSize,
}

impl ArchiveDownloader {
    fn spawn(self, mc_seqno: u32) -> ArchiveTask {
        // TODO: Use a proper backoff here?
        const INTERVAL: Duration = Duration::from_secs(10);

        let (tx, rx) = watch::channel(ArchiveTaskState::None);

        let guard = scopeguard::guard(tx, move |tx| {
            tracing::warn!(mc_seqno, "cancelled preloading archive");
            tx.send_modify(|prev| {
                if !matches!(prev, ArchiveTaskState::Finished(..)) {
                    *prev = ArchiveTaskState::Cancelled;
                }
            });
        });

        // NOTE: Use a separate downloader to prevent reference cycles
        let handle = tokio::spawn(async move {
            tracing::debug!(mc_seqno, "started preloading archive");
            scopeguard::defer! {
                tracing::debug!(mc_seqno, "finished preloading archive");
            }

            loop {
                match self.try_download(mc_seqno).await {
                    Ok(res) => {
                        let tx = scopeguard::ScopeGuard::into_inner(guard);
                        tx.send_modify(move |prev| *prev = ArchiveTaskState::Finished(res));
                        break;
                    }
                    Err(e) => {
                        tracing::error!("failed to preload: {e}");
                        tokio::time::sleep(INTERVAL).await;
                    }
                }
            }
        });

        ArchiveTask {
            archive_key: mc_seqno,
            rx,
            abort_handle: Arc::new(AbortOnDrop(handle.abort_handle())),
        }
    }

    async fn try_download(&self, seqno: u32) -> Result<Option<ArchiveInfo>> {
        if self.archive_ids.read().is_empty() {
            anyhow::bail!("archive downloader not ready seqno = {seqno}")
        }

        let id = self
            .archive_ids
            .read()
            .range(..=seqno)
            .next_back()
            .map(|(k, v)| (*k, *v));

        let (id, size) = match id {
            Some((id, size)) => {
                const ARCHIVE_PACKAGE_SIZE: u32 = 100;

                if seqno - id >= ARCHIVE_PACKAGE_SIZE {
                    // tracing::warn!(seqno, "archive too new");
                    // return Ok(None);
                    anyhow::bail!("archive too new seqno = {seqno}")
                }
                (id as _, NonZeroU64::new(size as _).unwrap())
            }
            None => anyhow::bail!("archive not found seqno = {seqno}"),
        };

        let writer = self.get_archive_writer(&size)?;
        let compressed = self.download_archive(id).await?;

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let mut writer = writer;
            let mut decompressed = Vec::new();
            ZstdDecompress::begin(compressed.as_ref())?.decompress(&mut decompressed)?;

            let mut verifier = ArchiveVerifier::default();
            verifier.write_verify(&decompressed)?;
            verifier.final_check()?;

            writer.write_all(&decompressed)?;
            writer.flush()?;

            let bytes = writer.try_freeze()?;

            let archive = match Archive::new(bytes) {
                Ok(array) => array,
                Err(e) => {
                    return Err(e);
                }
            };

            archive.check_mc_blocks_range()?;

            Ok(ArchiveInfo {
                archive: Arc::new(archive),
            })
        })
        .await?
        .map(Some)
    }

    pub async fn download_archive(&self, mc_seqno: u64) -> Result<Bytes> {
        tracing::debug!(mc_seqno, "downloading archive");

        let bytes = self
            .s3_client
            .get(&Path::from(mc_seqno.to_string()))
            .await?
            .bytes()
            .await?;

        Ok(bytes)
    }

    fn get_archive_writer(&self, size: &NonZeroU64) -> Result<ArchiveWriter> {
        Ok(if size.get() > self.memory_threshold.as_u64() {
            let file = self.storage.context().temp_files().unnamed_file().open()?;
            ArchiveWriter::File(std::io::BufWriter::new(file))
        } else {
            ArchiveWriter::Bytes(BytesMut::new().writer())
        })
    }

    async fn preload_archive_ids(&self) {
        let mut list_stream = self.s3_client.list(None);

        while let Some(item) = list_stream.next().await {
            let object = match item {
                Ok(item) => item,
                Err(e) => {
                    tracing::error!("failed to list S3 archives: {e}");
                    break;
                }
            };

            let id = object
                .location
                .to_string()
                .parse::<u32>()
                .expect("invalid location");

            self.archive_ids.write().insert(id, object.size as usize);
            tracing::info!(id, "loaded archive id");
        }
    }

    #[allow(dead_code)]
    #[tracing::instrument(name = "update_archive_ids", skip_all)]
    async fn update_archive_ids_task(self) {
        tracing::info!("started");
        scopeguard::defer! { tracing::info!("finished"); };

        // Start polling archive ids
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            let mut list_stream = self.s3_client.list(None);

            while let Some(item) = list_stream.next().await {
                let object = match item {
                    Ok(item) => item,
                    Err(e) => {
                        tracing::error!("failed to list S3 archives: {e}");
                        break;
                    }
                };

                let id = object
                    .location
                    .to_string()
                    .parse::<u32>()
                    .expect("invalid location");

                self.archive_ids.write().insert(id, object.size as usize);
            }

            interval.tick().await;
        }
    }
}

#[derive(Clone)]
struct ArchiveTask {
    archive_key: u32,
    rx: watch::Receiver<ArchiveTaskState>,
    abort_handle: Arc<AbortOnDrop>,
}

#[repr(transparent)]
struct AbortOnDrop(AbortHandle);

impl std::ops::Deref for AbortOnDrop {
    type Target = AbortHandle;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
enum ArchiveTaskState {
    #[default]
    None,
    Finished(Option<ArchiveInfo>),
    Cancelled,
}

impl BlockProvider for ArchiveBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = futures_util::future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id_relation))
    }

    fn cleanup_until(&self, mc_seqno: u32) -> Self::CleanupFut<'_> {
        self.inner.clear_outdated_archives(mc_seqno);
        futures_util::future::ready(Ok(()))
    }
}

enum ArchiveWriter {
    File(std::io::BufWriter<std::fs::File>),
    Bytes(bytes::buf::Writer<BytesMut>),
}

impl ArchiveWriter {
    fn try_freeze(self) -> Result<Bytes, std::io::Error> {
        match self {
            Self::File(file) => match file.into_inner() {
                Ok(mut file) => {
                    file.seek(std::io::SeekFrom::Start(0))?;
                    MappedFile::from_existing_file(file).map(Bytes::from_owner)
                }
                Err(e) => Err(e.into_error()),
            },
            Self::Bytes(data) => Ok(data.into_inner().freeze()),
        }
    }
}

impl std::io::Write for ArchiveWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(writer) => writer.write(buf),
            Self::Bytes(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.flush(),
            Self::Bytes(writer) => writer.flush(),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.write_all(buf),
            Self::Bytes(writer) => writer.write_all(buf),
        }
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> std::io::Result<()> {
        match self {
            Self::File(writer) => writer.write_fmt(fmt),
            Self::Bytes(writer) => writer.write_fmt(fmt),
        }
    }
}
