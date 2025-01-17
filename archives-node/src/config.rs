use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub s3_uploader: Option<ArchiveUploaderConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveUploaderConfig {}

impl Default for ArchiveUploaderConfig {
    fn default() -> Self {
        Self {}
    }
}
