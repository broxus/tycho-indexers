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

    /// S3 provider.
    ///
    /// Default: GCS
    pub provider: S3Provider,

    /// Default: 1 seconds.
    #[serde(with = "serde_helpers::humantime")]
    pub retry_delay: Duration,
}

impl Default for ArchiveUploaderConfig {
    fn default() -> Self {
        Self {
            provider: Default::default(),
            bucket_name: "archives".to_owned(),
            retry_delay: Duration::from_secs(1),
        }
    }
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
