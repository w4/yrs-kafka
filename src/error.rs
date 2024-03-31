use thiserror::Error;

#[derive(Error, Debug)]
pub enum InitError {
    #[error("failed to create kafka producer: {0}")]
    CreateProducer(rdkafka::error::KafkaError),
    #[error("failed to open rocksdb instance: {0}")]
    OpenRocksDb(rocksdb::Error),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to send message to kafka producer: {0}")]
    SendProducer(rdkafka::error::KafkaError),
    #[error("failed to read state from rocksdb: {0}")]
    ReadRocksDb(rocksdb::Error),
    #[error("failed to join on spawned task: {0}")]
    SpawnBlocking(tokio::task::JoinError),
}

#[derive(Error, Debug)]
pub(super) enum InternalError {
    #[error("failed to create topic stream reader: {0}")]
    TopicReader(rdkafka::error::KafkaError),
    #[error("failed to subscribe to topic: {0}")]
    TopicSubscribe(rdkafka::error::KafkaError),
    #[error("failed to read from topic: {0}")]
    ReadTopic(rdkafka::error::KafkaError),
    #[error("failed to merge update into rocksdb store: {0}")]
    MergeUpdate(rocksdb::Error),
    #[error("failed to read from rocksdb store after merge: {0}")]
    ReadAfterMerge(rocksdb::Error),
    #[error("bad state, data being unavailable after rocksdb merge")]
    MissingUnexpected,
    #[error("failed to update compacted topic: {0}")]
    UpdateCompacted(rdkafka::error::KafkaError),
    #[error("failed to commit offset: {0}")]
    CommitOffset(rdkafka::error::KafkaError),
}
