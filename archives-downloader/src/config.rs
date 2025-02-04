use serde::{Deserialize, Serialize};

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
