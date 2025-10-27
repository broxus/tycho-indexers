use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub uploader: Option<UploaderConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UploaderConfig {
    /// Timeout to retry upload.
    ///
    /// Default: 1 seconds.
    #[serde(default = "default_retry_delay", with = "serde_helpers::humantime")]
    pub retry_delay: Duration,

    /// Max concurrency uploading tasks.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,

    /// Enable duplication data in S3.
    ///
    /// Default: false.
    #[serde(default = "default_enable_duplication")]
    pub enable_duplication: bool,

    /// Number of recent archives to re-upload
    #[serde(default = "default_last_archives_to_upload")]
    pub last_archives_to_upload: usize,
}

fn default_retry_delay() -> Duration {
    Duration::from_secs(1)
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
