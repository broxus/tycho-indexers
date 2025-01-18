use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub s3_uploader: Option<ArchiveUploaderConfig>,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveUploaderConfig {
    pub credentials_path: String,

    /// Default: 1 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub retry_delay: Duration,
}

impl Default for ArchiveUploaderConfig {
    fn default() -> Self {
        Self {
            credentials_path: Default::default(),
            retry_delay: Duration::from_secs(1),
        }
    }
}
