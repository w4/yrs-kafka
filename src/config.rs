use std::path::PathBuf;

use rdkafka::ClientConfig;

#[derive(Clone)]
pub struct Config {
    pub path: PathBuf,
    pub kafka: KafkaConfig,
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub group_id: String,
}

impl From<KafkaConfig> for ClientConfig {
    fn from(value: KafkaConfig) -> Self {
        let mut config = Self::new();
        config.set("group.id", value.group_id);
        config.set("bootstrap.servers", value.brokers.join(","));
        config
    }
}
