# yrs-kafka

[Yrs] synchronization and persistence using Kafka and an ephemeral RocksDB instance fed by a
compacted topic.

[Yrs]: https://github.com/y-crdt/y-crdt/

## Updates

Documents are assigned a de facto "owner" via partition ownership of the changelog topic. All changes to a given
document are handled by the owner and merged into the compacted topic, which is consumed by other instances of
the service.

Changes published to the compacted topic are persisted into the local instance's ephemeral RocksDB, except in the
case of document owners which merge into their RocksDB instance upon read from the changelog.

![Update diagram](./.github/img/update-flow.png)

## Reads

Reading document state is performed entirely using the instance's local ephemeral RocksDB.

![Read diagram](./.github/img/read-flow.png)
