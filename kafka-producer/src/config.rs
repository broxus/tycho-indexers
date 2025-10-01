use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserConfig {
    pub kafka: Option<KafkaConsumerConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConsumerConfig {
    pub topic: String,
    pub brokers: String,
    pub group_id: String,
    #[serde(default)]
    pub security_config: Option<SecurityConfig>,
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u32,
    #[serde(default = "default_offset_commit_interval_ms")]
    pub message_timeout_ms: Option<u32>,
    pub message_max_size: Option<usize>,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            topic: "".to_string(),
            brokers: "".to_string(),
            group_id: "tycho-kafka-producer".to_string(),
            security_config: None,
            session_timeout_ms: default_session_timeout_ms(),
            message_timeout_ms: default_offset_commit_interval_ms(),
            message_max_size: Some(2_000_000),
        }
    }
}

fn default_offset_commit_interval_ms() -> Option<u32> {
    Some(5000)
}

fn default_session_timeout_ms() -> u32 {
    6000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum SecurityConfig {
    #[cfg(feature = "ssl")]
    Ssl(SslConfig),
}

#[cfg(feature = "ssl")]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SslConfig {
    pub security_protocol: String,
    pub ssl_ca_location: String,
    pub ssl_key_location: String,
    pub ssl_certificate_location: String,
    #[serde(default)]
    pub enable_ssl_certificate_verification: Option<bool>,
}
