use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub s3_uploader: Option<ArchiveUploaderConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveUploaderConfig {
    /// S3 bucket name.
    ///
    /// Default: 'archives'.
    #[serde(default = "default_bucket_name")]
    pub bucket_name: String,

    /// S3 provider.
    ///
    /// Default: GCS
    #[serde(default)]
    pub provider: S3Provider,

    /// Timeout to retry upload.
    ///
    /// Default: 1 seconds.
    #[serde(default = "default_retry_delay", with = "serde_helpers::humantime")]
    pub retry_delay: Duration,

    /// Upload chunk size.
    ///
    /// Default: 10 MB.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: ByteSize,

    /// Max concurrency uploading tasks.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,

    /// Number of recent archives to re-upload
    #[serde(default = "default_last_archives_to_upload")]
    pub last_archives_to_upload: usize,

    /// Enable duplication data in S3.
    ///
    /// Default: false.
    #[serde(default = "default_enable_duplication")]
    pub enable_duplication: bool,
}

fn default_bucket_name() -> String {
    "archives".to_owned()
}

fn default_retry_delay() -> Duration {
    Duration::from_secs(1)
}

fn default_chunk_size() -> ByteSize {
    ByteSize::mb(10)
}

fn default_enable_duplication() -> bool {
    false
}

fn default_max_concurrency() -> usize {
    std::cmp::min(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8),
        16, // Reasonable upper limit
    )
}

fn default_last_archives_to_upload() -> usize {
    3
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum S3Provider {
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

impl Default for S3Provider {
    fn default() -> Self {
        Self::Gcs {
            credentials_path: "credentials.json".to_owned(),
        }
    }
}
