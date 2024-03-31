#![allow(clippy::unwrap_used, clippy::future_not_send)]

use std::time::Duration;

use rdkafka::{
    config::FromClientConfig,
    message::{Header, OwnedHeaders},
    producer::FutureRecord,
    util::{DefaultRuntime, Timeout},
};
use testcontainers::{
    clients,
    core::{env, ExecCommand, WaitFor},
    Image, ImageArgs, RunnableImage,
};
use uuid::Uuid;
use yrs::{updates::decoder::Decode, Doc, GetString, Text, Transact, Update};

use crate::{
    config::{Config, KafkaConfig},
    INSTANCE_ID_HEADER,
};

#[tokio::test(flavor = "multi_thread")]
async fn test() {
    // TODO: figure out how to expose redpanda on a non-default port so these
    //  tests can be ran in parallel
    eprintln!("running updates_originating_from_current_instance");
    updates_originating_from_current_instance().await;
    eprintln!("running updates_originating_from_other_instance");
    updates_originating_from_other_instance().await;
    eprintln!("running updates_originating_from_compacted_topic");
    updates_originating_from_compacted_topic().await;
}

async fn updates_originating_from_current_instance() {
    let docker = clients::Cli::new::<env::Os>();
    let container = docker.run(RedpandaContainer::latest());
    let temp_dir = tempfile::tempdir().unwrap();

    container.exec(RedpandaContainer::cmd_create_topic("yjs-changelog", 1));
    container.exec(RedpandaContainer::cmd_create_topic("yjs-compacted", 1));

    let kafka_config = KafkaConfig {
        brokers: vec![format!("localhost:{}", container.get_host_port_ipv4(9092))],
        group_id: Uuid::new_v4().to_string(),
        compacted_topic: "yjs-compacted".to_string(),
        changelog_topic: "yrs-changelog".to_string(),
    };

    let y = crate::start(Config {
        db_path: temp_dir.path().to_path_buf(),
        kafka: kafka_config,
    })
    .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut recv = y.subscribe();

    let doc = Doc::new();
    let text = doc.get_or_insert_text("article");
    let changes = {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "hello");
        text.insert(&mut txn, 5, " world");
        txn.encode_update_v1()
    };

    y.update(b"my-document", changes).await.unwrap();
    let changed_key = recv.recv().await.unwrap();
    assert_eq!(changed_key.as_ref(), b"my-document");

    let updates = y.load_document(b"my-document").await.unwrap();
    let updates = updates.get().as_ref().unwrap();
    let doc = Doc::new();
    doc.transact_mut()
        .apply_update(Update::decode_v1(updates.as_ref()).unwrap());
    assert_eq!(
        doc.get_or_insert_text("article")
            .get_string(&doc.transact()),
        "hello world"
    );

    let changes = {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 11, " testing!");
        txn.encode_update_v1()
    };

    y.update(b"my-document", changes).await.unwrap();
    let changed_key = recv.recv().await.unwrap();
    assert_eq!(changed_key.as_ref(), b"my-document");

    let updates = y.load_document(b"my-document").await.unwrap();
    let updates = updates.get().as_ref().unwrap();
    let doc = Doc::new();
    doc.transact_mut()
        .apply_update(Update::decode_v1(updates.as_ref()).unwrap());

    assert_eq!(
        doc.get_or_insert_text("article")
            .get_string(&doc.transact()),
        "hello world testing!"
    );
}

async fn updates_originating_from_other_instance() {
    let docker = clients::Cli::new::<env::Os>();
    let container = docker.run(RedpandaContainer::latest());

    container.exec(RedpandaContainer::cmd_create_topic("yjs-changelog", 1));
    container.exec(RedpandaContainer::cmd_create_topic("yjs-compacted", 1));

    let kafka_config = KafkaConfig {
        brokers: vec![format!("localhost:{}", container.get_host_port_ipv4(9092))],
        group_id: Uuid::new_v4().to_string(),
        changelog_topic: "yjs-changelog".to_string(),
        compacted_topic: "yjs-compacted".to_string(),
    };

    let temp_dir1 = tempfile::tempdir().unwrap();
    let y1 = crate::start(Config {
        db_path: temp_dir1.path().to_path_buf(),
        kafka: kafka_config.clone(),
    })
    .unwrap();

    let temp_dir2 = tempfile::tempdir().unwrap();
    let y2 = crate::start(Config {
        db_path: temp_dir2.path().to_path_buf(),
        kafka: kafka_config,
    })
    .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut recv = y2.subscribe();

    let doc = Doc::new();
    let text = doc.get_or_insert_text("article");
    let changes = {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "hello");
        text.insert(&mut txn, 5, " world");
        txn.encode_update_v1()
    };

    y1.update(b"my-document", changes).await.unwrap();

    let changed_key = recv.recv().await.unwrap();
    assert_eq!(changed_key.as_ref(), b"my-document");

    let updates = y2.load_document(b"my-document").await.unwrap();
    let updates = updates.get().as_ref().unwrap();
    let doc = Doc::new();
    doc.transact_mut()
        .apply_update(Update::decode_v1(updates.as_ref()).unwrap());
    assert_eq!(
        doc.get_or_insert_text("article")
            .get_string(&doc.transact()),
        "hello world"
    );
}

async fn updates_originating_from_compacted_topic() {
    let docker = clients::Cli::new::<env::Os>();
    let container = docker.run(RedpandaContainer::latest());
    let temp_dir = tempfile::tempdir().unwrap();

    container.exec(RedpandaContainer::cmd_create_topic("yjs-changelog", 1));
    container.exec(RedpandaContainer::cmd_create_topic("yjs-compacted", 1));

    let kafka_config = KafkaConfig {
        brokers: vec![format!("localhost:{}", container.get_host_port_ipv4(9092))],
        group_id: Uuid::new_v4().to_string(),
        compacted_topic: "yjs-compacted".to_string(),
        changelog_topic: "yrs-changelog".to_string(),
    };

    let payload = {
        let doc = Doc::new();
        let text = doc.get_or_insert_text("article");

        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "hello");
        text.insert(&mut txn, 5, " world");
        txn.encode_update_v1()
    };

    let producer = rdkafka::producer::FutureProducer::<_, DefaultRuntime>::from_config(
        &kafka_config.clone().into(),
    )
    .unwrap();
    let record = FutureRecord::to("yjs-compacted")
        .key(b"my-document")
        .headers(OwnedHeaders::new().insert(Header {
            key: INSTANCE_ID_HEADER,
            value: Some(b"test"),
        }))
        .payload(&payload);
    producer.send(record, Timeout::Never).await.unwrap();

    let y = crate::start(Config {
        db_path: temp_dir.path().to_path_buf(),
        kafka: kafka_config,
    })
    .unwrap();

    let mut recv = y.subscribe();
    let changed_key = recv.recv().await.unwrap();
    assert_eq!(changed_key.as_ref(), b"my-document");

    let updates = y.load_document(b"my-document").await.unwrap();
    let updates = updates.get().as_ref().unwrap();
    let doc = Doc::new();
    doc.transact_mut()
        .apply_update(Update::decode_v1(updates.as_ref()).unwrap());
    assert_eq!(
        doc.get_or_insert_text("article")
            .get_string(&doc.transact()),
        "hello world"
    );
}

#[derive(Default, Clone, Debug)]
pub struct RedpandaContainer;

impl RedpandaContainer {
    fn latest() -> RunnableImage<Self> {
        RunnableImage::from(Self).with_mapped_port((9092, 9092))
    }

    pub fn cmd_create_topic(topic_name: &str, partitions: i32) -> ExecCommand {
        let ready_conditions = vec![
            WaitFor::StdErrMessage {
                message: String::from("Create topics"),
            },
            WaitFor::Duration {
                length: std::time::Duration::from_secs(1),
            },
        ];

        ExecCommand {
            cmd: format!("rpk topic create {topic_name} -p {partitions}"),
            ready_conditions,
        }
    }
}

impl Image for RedpandaContainer {
    type Args = Self;

    fn name(&self) -> String {
        "redpandadata/redpanda".to_string()
    }

    fn tag(&self) -> String {
        "latest".to_string()
    }

    fn entrypoint(&self) -> Option<String> {
        Some("sh".to_string())
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("Initialized cluster_id to ")]
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![9092]
    }
}

impl ImageArgs for RedpandaContainer {
    fn into_iterator(self) -> Box<dyn Iterator<Item = String>> {
        Box::new([
            "-c",
            "/usr/bin/rpk redpanda start --mode dev-container --node-id 0 --set redpanda.auto_create_topics_enabled=true"
        ].map(str::to_string).into_iter())
    }
}
