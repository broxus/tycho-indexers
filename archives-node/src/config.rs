use std::time::Duration;

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
    pub bucket_name: String,

    /// S3 credentials.
    ///
    /// Default: 'credentials.json'
    pub credentials_path: String,

    /// Default: 1 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub retry_delay: Duration,
}

impl Default for ArchiveUploaderConfig {
    fn default() -> Self {
        Self {
            bucket_name: "archives".to_owned(),
            credentials_path: "credentials.json".to_owned(),
            retry_delay: Duration::from_secs(1),
        }
    }
}
