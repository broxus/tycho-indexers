use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tycho_core::block_strider::{ArchiveBlockProviderConfig, StarterConfig};
use tycho_storage::StorageConfig;
use tycho_util::cli::config::ThreadPoolConfig;
use tycho_util::cli::logger::LoggerConfig;
use tycho_util::cli::metrics::MetricsConfig;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub bucket_name: String,
    pub s3_provider: ArchiveProviderConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ArchiveProviderConfig {
    // TODO: add client options and move credentials to env
    #[serde(rename = "aws")]
    Aws {
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
        allow_http: bool,
    },

    #[serde(rename = "gcs")]
    Gcs { credentials_path: String },
}

impl Default for ArchiveProviderConfig {
    fn default() -> Self {
        Self::Gcs {
            credentials_path: "credentials.json".to_owned(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct NodeConfig<T> {
    pub storage: StorageConfig,

    pub archive_block_provider: ArchiveBlockProviderConfig,

    pub metrics: Option<MetricsConfig>,

    pub threads: ThreadPoolConfig,

    pub profiling: MemoryProfilingConfig,

    pub logger_config: LoggerConfig,

    pub starter: StarterConfig,

    #[serde(flatten)]
    pub user_config: T,
}

impl<T> Default for NodeConfig<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            archive_block_provider: Default::default(),
            metrics: Some(MetricsConfig::default()),
            threads: ThreadPoolConfig::default(),
            profiling: Default::default(),
            logger_config: Default::default(),
            starter: Default::default(),
            user_config: Default::default(),
        }
    }
}

impl<T> NodeConfig<T>
where
    T: Serialize + DeserializeOwned + Default,
{
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<NodeConfig<T>> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryProfilingConfig {
    pub profiling_dir: PathBuf,
}
