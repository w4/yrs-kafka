use thiserror::Error;

/// Non-recoverable errors during initialisation.
#[derive(Error, Debug)]
pub enum InitError {
    /// The [rdkafka] producer could not be properly initialised.
    #[error("failed to create kafka producer: {0}")]
    CreateProducer(rdkafka::error::KafkaError),
    /// The [rocksdb] database could not be opened, consider deleting it.
    #[error("failed to open rocksdb instance: {0}")]
    OpenRocksDb(rocksdb::Error),
}

/// Errors thrown during `yrs-kafka` runtime.
#[derive(Error, Debug)]
pub enum Error {
    /// A message could not be sent by [rdkafka].
    #[error("failed to send message to kafka producer: {0}")]
    SendProducer(rdkafka::error::KafkaError),
    /// State could not be successfully read by [rocksdb].
    #[error("failed to read state from rocksdb: {0}")]
    ReadRocksDb(rocksdb::Error),
    /// A blocking task failed to be ran in the background.
    #[error("failed to join on spawned task: {0}")]
    SpawnBlocking(tokio::task::JoinError),
}

/// Recoverable internal errors thrown by background tasks.
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
    #[error("failed to join on tokio task: {0}")]
    Join(tokio::task::JoinError),
    #[error("failed to read from rocksdb store after merge: {0}")]
    ReadAfterMerge(rocksdb::Error),
    #[error("bad state, data being unavailable after rocksdb merge")]
    MissingUnexpected,
    #[error("failed to update compacted topic: {0}")]
    UpdateCompacted(rdkafka::error::KafkaError),
    #[error("failed to commit offset: {0}")]
    CommitOffset(rdkafka::error::KafkaError),
}
