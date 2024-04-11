#![doc = include_str!("../README.md")]
#![deny(
    anonymous_parameters,
    bare_trait_objects,
    elided_lifetimes_in_paths,
    missing_copy_implementations,
    rust_2018_idioms,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_extern_crates,
    unused_import_braces,
    clippy::all,
    clippy::cargo,
    clippy::dbg_macro,
    clippy::float_cmp_const,
    clippy::get_unwrap,
    clippy::mem_forget,
    clippy::nursery,
    clippy::pedantic,
    clippy::todo,
    clippy::unwrap_used
)]
#![allow(clippy::multiple_crate_versions, clippy::module_name_repetitions)]

/// Configuration for [`start`].
pub mod config;
/// Errors exposed by `yrs-kafka`.
pub mod error;
mod notify_stream;
#[cfg(test)]
mod test;

use std::{
    collections::HashMap,
    future::Future,
    ops::{Deref, Mul},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use futures::FutureExt;
use log::error;
use parking_lot::Mutex;
use rand::Rng;
use rdkafka::{
    config::FromClientConfig,
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    message::{Header, Headers, OwnedHeaders},
    producer::FutureRecord,
    util::{DefaultRuntime, Timeout},
    ClientConfig, Message,
};
use tokio::{sync::Notify, task::JoinSet, time::Instant};
use uuid::Uuid;
use yoke::Yoke;
use yrs::{
    updates::{decoder::Decode, encoder::Encode},
    Doc, ReadTxn, Transact, Update,
};

pub use crate::notify_stream::NotifyStream;
use crate::{
    config::Config,
    error::{Error, InitError, InternalError},
    yoked::PinnableSlice,
};

const INSTANCE_ID_HEADER: &str = "instance-id";

/// Represents a running instance of `YrsKafka` along with its background
/// tasks.
#[derive(Clone)]
#[allow(dead_code)]
pub struct YrsKafkaRunning {
    inner: YrsKafka,
    tasks: Arc<JoinSet<()>>,
}

impl Deref for YrsKafkaRunning {
    type Target = YrsKafka;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Main entrypoint to yrs-kafka.
///
/// # Errors
///
/// This function will return an error during initialisation, errors
/// post-initialisation are internally recoverable.
pub fn start(config: Config) -> Result<YrsKafkaRunning, InitError> {
    let store = YrsKafka::new(&config)?;

    let mut join_set = JoinSet::new();
    join_set.spawn(watchdog(
        "changelog",
        read_changelog_stream,
        config.clone(),
        store.clone(),
    ));
    join_set.spawn(watchdog(
        "compacted",
        read_compacted_topic,
        config,
        store.clone(),
    ));

    Ok(YrsKafkaRunning {
        inner: store,
        tasks: Arc::new(join_set),
    })
}

/// Reads the compacted topic of documents.
async fn read_compacted_topic(config: Config, store: YrsKafka) -> Result<(), InternalError> {
    let mut kafka_config = ClientConfig::from(config.kafka.clone());
    kafka_config.set("group.id", Uuid::new_v4());
    kafka_config.set("enable.auto.commit", "false");
    kafka_config.set("enable.auto.offset.store", "false");
    kafka_config.set("auto.offset.reset", "earliest");

    let stream =
        StreamConsumer::<DefaultConsumerContext, DefaultRuntime>::from_config(&kafka_config)
            .map_err(InternalError::TopicReader)?;
    stream
        .subscribe(&[&config.kafka.compacted_topic])
        .map_err(InternalError::TopicSubscribe)?;

    loop {
        let msg = stream.recv().await.map_err(InternalError::ReadTopic)?;

        let Some(instance_id) = msg
            .headers()
            .iter()
            .flat_map(|v| v.iter())
            .find(|v| v.key == INSTANCE_ID_HEADER)
        else {
            continue;
        };

        if instance_id.value == Some(&store.instance_id.as_u128().to_be_bytes()) {
            continue;
        }

        let (Some(key), Some(payload)) = (msg.key(), msg.payload()) else {
            continue;
        };

        let key_copy = Box::from(key);
        let payload = Box::from(payload);
        let rocksdb = store.rocksdb.clone();

        tokio::task::spawn_blocking(move || {
            rocksdb
                .merge(key_copy, payload)
                .map_err(InternalError::MergeUpdate)
        })
        .await
        .map_err(InternalError::Join)??;

        {
            let mut document_state = store.document_updated.lock();

            if let Some(subscribers) = document_state.get(key) {
                subscribers.notify_waiters();
            } else {
                document_state.insert(key.into(), Arc::new(Notify::new()));
            }
        }
    }
}

/// Reads the changelog stream which is partitioned by document id, the
/// partitions owned by the current instance infer which documents the
/// current instance owns.
///
/// Upon reading a changelog message, the update will be applied to the
/// relevant document, after which the document is serialised and written
/// to the compacted topic to be read by other instances.
async fn read_changelog_stream(config: Config, store: YrsKafka) -> Result<(), InternalError> {
    let stream = StreamConsumer::<DefaultConsumerContext, DefaultRuntime>::from_config(
        &config.kafka.clone().into(),
    )
    .map_err(InternalError::TopicReader)?;
    stream
        .subscribe(&[&config.kafka.changelog_topic])
        .map_err(InternalError::TopicSubscribe)?;

    loop {
        let msg = stream.recv().await.map_err(InternalError::ReadTopic)?;

        let (Some(key), Some(payload)) = (msg.key(), msg.payload()) else {
            continue;
        };

        let rocksdb = store.rocksdb.clone();
        let key_copy = Box::from(key);
        let payload = Box::from(payload);

        // merge the change into our source of truth and read it back for pushing
        // into the compacted topic
        let value: Yoke<PinnableSlice<'static>, _> = tokio::task::spawn_blocking(move || {
            rocksdb
                .merge(&key_copy, payload)
                .map_err(InternalError::MergeUpdate)?;

            Yoke::try_attach_to_cart(rocksdb, |v| {
                v.get_pinned(&key_copy)
                    .map_err(InternalError::ReadAfterMerge)?
                    .map(yoked::PinnableSlice)
                    .ok_or(InternalError::MissingUnexpected)
            })
        })
        .await
        .map_err(InternalError::Join)??;
        let value = value.get();

        // return the doc change event to all subscribers in the consumer
        {
            let mut document_state = store.document_updated.lock();

            if let Some(subscribers) = document_state.get(key) {
                subscribers.notify_waiters();
            } else {
                document_state.insert(key.into(), Arc::new(Notify::new()));
            }
        }

        // send the change to the compacted topic for other instances
        // to read into their store
        let record = FutureRecord::to(&config.kafka.compacted_topic)
            .key(key)
            .headers(OwnedHeaders::new().insert(Header {
                key: INSTANCE_ID_HEADER,
                value: Some(&store.instance_id.as_u128().to_be_bytes()),
            }))
            .payload(value.as_ref());
        store
            .producer
            .send(record, Timeout::After(Duration::from_secs(1)))
            .await
            .map_err(|(e, _)| InternalError::UpdateCompacted(e))?;

        stream
            .commit_message(&msg, CommitMode::Async)
            .map_err(InternalError::CommitOffset)?;
    }
}

/// Main interface of `yrs-kafka`.
#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct YrsKafka {
    instance_id: Uuid,
    rocksdb: Arc<rocksdb::DB>,
    producer: rdkafka::producer::FutureProducer,
    document_updated: Arc<Mutex<HashMap<Arc<[u8]>, Arc<Notify>>>>,
    changelog_topic: Arc<str>,
}

impl YrsKafka {
    fn new(config: &Config) -> Result<Self, InitError> {
        let mut rocksdb_options = rocksdb::Options::default();
        rocksdb_options.create_if_missing(true);
        rocksdb_options.set_merge_operator_associative("yjs", |_key, value, operands| {
            let document = Doc::new();

            {
                let mut txn = document.transact_mut();

                if let Some(change) = value {
                    txn.apply_update(Update::decode_v1(change).expect("failed to decode update"));
                }

                for change in operands {
                    txn.apply_update(Update::decode_v1(change).expect("failed to decode update"));
                }
            }

            let res = Some(document.transact().store().encode_v1());
            res
        });

        let rocksdb = Arc::new(
            rocksdb::DB::open(&rocksdb_options, &config.db_path).map_err(InitError::OpenRocksDb)?,
        );

        let mut kafka_config: ClientConfig = config.kafka.clone().into();
        kafka_config.remove("group.id");
        let producer = rdkafka::producer::FutureProducer::from_config(&kafka_config)
            .map_err(InitError::CreateProducer)?;

        Ok(Self {
            instance_id: Uuid::new_v4(),
            rocksdb,
            producer,
            document_updated: Arc::new(Mutex::new(HashMap::new())),
            changelog_topic: Arc::from(config.kafka.changelog_topic.to_string()),
        })
    }

    /// Subscribes to document changes to the given id, yielding the new
    /// state vector.
    #[must_use]
    pub fn subscribe(&self, id: &[u8]) -> NotifyStream {
        let mut document_states = self.document_updated.lock();

        document_states.get(id).cloned().map_or_else(
            move || {
                let notify = Arc::new(Notify::new());
                document_states.insert(Arc::from(id), notify.clone());
                NotifyStream::new(notify)
            },
            NotifyStream::new,
        )
    }

    /// Pushes an update for a document with the given `id` to the changelog
    /// in Kafka.
    ///
    /// # Errors
    ///
    /// Returns an error if the changelog couldn't be written to.
    pub async fn update(&self, id: &[u8], payload: Vec<u8>) -> Result<(), Error> {
        let record = FutureRecord::to(&self.changelog_topic)
            .key(id)
            .payload(&payload);

        self.producer
            .send(record, Timeout::After(Duration::from_secs(1)))
            .await
            .map_err(|(e, _)| Error::SendProducer(e))?;

        if let Err(e) = self.rocksdb.merge(id, payload) {
            error!("Failed to update local state: {e}");
        } else {
            let mut document_state = self.document_updated.lock();

            if let Some(subscribers) = document_state.get(id) {
                subscribers.notify_waiters();
            } else {
                document_state.insert(id.into(), Arc::new(Notify::new()));
            }
        }

        Ok(())
    }

    /// Loads every update for a document into memory from the local
    /// `RocksDB` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the `RocksDB` read fails.
    pub async fn load_document(
        &self,
        id: impl Into<Vec<u8>> + Send,
    ) -> Result<Yoke<Option<yoked::PinnableSlice<'static>>, Arc<rocksdb::DB>>, Error> {
        let rocksdb = self.rocksdb.clone();
        let id = id.into();

        tokio::task::spawn_blocking(move || {
            Yoke::try_attach_to_cart(rocksdb, |v| {
                Ok(v.get_pinned(id)
                    .map_err(Error::ReadRocksDb)?
                    .map(yoked::PinnableSlice))
            })
        })
        .await
        .map_err(Error::SpawnBlocking)
        .and_then(std::convert::identity)
    }
}

/// [Yoked][yoke] data structures exposed by `yrs-kafka`.
#[allow(clippy::mem_forget)]
pub mod yoked {
    use std::ops::Deref;

    use rocksdb::DBPinnableSlice;
    use yoke::Yokeable;

    /// Holds a value from `RocksDB` within a `PinnableSlice` to avoid
    /// unnecessary memcpys.
    #[derive(Yokeable)]
    pub struct PinnableSlice<'a>(pub(crate) DBPinnableSlice<'a>);

    impl<'a> Deref for PinnableSlice<'a> {
        type Target = DBPinnableSlice<'a>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
}

/// Non-returning function that restarts `func` whenever it returns
/// unexpectedly, with an exponential backoff.
async fn watchdog<F>(
    kind: &str,
    func: impl Fn(Config, YrsKafka) -> F + Send,
    config: Config,
    yrs_kafka: YrsKafka,
) where
    F: Future<Output = Result<(), InternalError>> + Send + 'static,
{
    let mut last_restarted = Instant::now();
    let mut restarted_count = 0_u16;

    loop {
        let res = AssertUnwindSafe((func)(config.clone(), yrs_kafka.clone()))
            .catch_unwind()
            .await;

        // if we last restarted more than 5 minutes ago, we'll assume that
        // the service was stable for at least a little while and reset our
        // restarted count.
        if last_restarted.elapsed() > Duration::from_secs(300) {
            restarted_count = 0;
        }

        last_restarted = Instant::now();
        restarted_count = restarted_count.saturating_add(1);

        let next_try = f32::from(restarted_count)
            .exp2()
            .mul(rand::thread_rng().gen_range(0.5..1.5))
            .min(120.0);
        let next_try = Duration::from_secs_f32(next_try);

        error!("{kind} consumer has unexpectedly shutdown with output {res:?}, restarting in {next_try:?}");
        tokio::time::sleep(next_try).await;
    }
}
