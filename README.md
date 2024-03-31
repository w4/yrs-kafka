# yrs-kafka

[Yrs] synchronization and persistence using Kafka and an ephemeral RocksDB instance fed by a
compacted topic.

[Yrs]: https://github.com/y-crdt/y-crdt/

## Usage

### Topic configuration

Two topics are required to use yrs-kafka, a compacted topic and a changelog topic. To configure
these two topics using Redpanda, run the following commands:

```bash
rpk topic create y-compacted -c cleanup.policy=compact
rpk topic create y-changelog 
```

### In application

```rust,no_run
use yrs::{updates::decoder::Decode, Text, Transact, Update};

async fn run() {
    let yrs_kafka = yrs_kafka::start(yrs_kafka::config::Config {
        db_path: "/tmp/yrs-kafka".into(),
        kafka: yrs_kafka::config::KafkaConfig {
            brokers: vec!["localhost:9092".to_string()],
            group_id: "yrs-kafka".to_string(),
            changelog_topic: "y-changelog".to_string(),
            compacted_topic: "y-compacted".to_string(),
        },
    })
    .unwrap();

    let doc = yrs::Doc::new();

    let update = yrs_kafka.load_document("my-document-id").await.unwrap();
    let update = update.get().as_deref().unwrap();
    doc.transact_mut().apply_update(Update::decode_v1(update).unwrap());

    let update = {
        let text = doc.get_or_insert_text("article");
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "hello");
        text.insert(&mut txn, 5, " world");
        txn.encode_update_v1()
    };

    yrs_kafka.update(b"my-document-id", update).await.unwrap();
}
```

## Design

### Updates

Documents are assigned a de facto "owner" via partition ownership of the changelog topic. All changes to a given
document are handled by the owner and merged into the compacted topic, which is consumed by other instances of
the service.

Changes published to the compacted topic are persisted into the local instance's ephemeral RocksDB, except in the
case of document owners which merge into their RocksDB instance upon read from the changelog.

![Update diagram](./.github/img/update-flow.png)

### Reads

Reading document state is performed entirely using the instance's local ephemeral RocksDB.

![Read diagram](./.github/img/read-flow.png)
