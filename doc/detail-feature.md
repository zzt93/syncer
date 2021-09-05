### Input -- DataSource

- Support listening to both MySQL & MongoDB & DRDS of Aliyun (https://www.aliyun.com/product/drds)
- If Syncer fail to connect to input data source, will abort
- MySQL master source filter:
  - Schema filter (naming as `repos`), support regex
    - If a table match multiple schemas & table (because the usage of regex), an error message will be logged and
      syncer will use anyone that match filter column
  - Table name filter
  - Interested column filter
    - If a change event go through column filter, and only primary key is left:
    - If change event type is `UPDATE`, then discard this change event -- because not support update id now;
    - Other change event type, keep it.
  - In a `UPDATE`, all interested column will be received even no change (different from `MongoDB`)
  - automatic primary key detection and set into `SyncData#id`
  - Support reading from binlog file to do data recovering in case of loss of data (`input[x].file`)
  - Support specify binlog file/position to start reading (`input[x].connection.syncMeta[]`)
- MongoDB master source filter:
  - Version: 3.x, 4.0
    - Only 4.0 support field removed detection and sync (Because the limitation of ES/MySQL, it always means setting field to null in output target which may not what you want) 
  - Database filter (naming as `repos`), support regex
  - Collection name filter
  - In a `UPDATE`, only changed column will be received (different from `MySQL`)
  - automatic `_id` detection and set into `SyncData#id`
  - If a change event match multiple schemas & table, we will use the first match (config file order) to filter/output,
  i.e. the specific `repo` config will override the regex `repo` config
  - If a change event go through column filter, and only primary key is left:
    - If change event type is `UPDATE`, then discard this change event -- because not support update id now;
    - Other change event type, keep it.
  - If config user/password for auth, it should have permission of `[listDatabases, find]`
  - Only support listening first level field (Because MongoDB store json, it may have multiple levels)
- DRDS:
  - Same config as MySQL, but need to connect directly to RDS's MySQL because DRDS not support binlog dump
  - Remember to fetch partition key in `fields`

---

- Remember where we leave last time by writing file/position of binlog/oplog, and resume from there so as to avoid any data loss
  - More than once (at-least-once): we can ensure the at least once semantics now, so you need to make sure your output channel (the `consumer` of syncer output)
  is **idempotent** and your destination can handle it without dup. Counterexample: a table without primary key definitely
  can't handle it and cause duplicate data soon or later.
- Multiple consumer can share a common connection to same data source, i.e. MySQL/MongoDB, to reduce the
burden of remote master
- Automatically skip synced item for consumers according to register info 

---

After data items come out from `Input` module, it is converted to `SyncData`(s) -- the abstraction of
a single data change. In other words, a single binlog item may contain multiple line change and convert
to multiple `SyncData`s.

### Filter -- Operation

Manipulate `SyncData` via :

- `sourcePath`: write a java class to handle `SyncData` (for more details, see filter part of *[Consumer Pipeline Config](config/consumer-filter.md)*)
- or skip it if no action needed


### Output -- DataSink

- If output channel meet too many failure/error (exceeds `countLimit`), it will abort and change health to `red` 
- If fail to connect to output channel, will retry every 2**n seconds
- Elasticsearch
  - Version: 5.x
  - Bulk operation
  - Update/Delete documents by `UpdateByQuery` or `DeleteByQuery`
  - Join/merge documents from different source when push to ES<sup>[1](#join_in_es)</sup>
    - ExtraQuery: do extra query to fetch extra needed info
      - Support multiple extra dependent query via special mark `$var$`
    - One to many relationship (parent-child relationship in ES)for document in different index
    - Self referential relationship handle
  - Add `upsert` support, fix `DocumentMissingException` use `upsert`, can be used in following two scenarios
    - Init load for data, by creating index manually and update synced field to ES (only support `MySQL` input) 
    - Fix some un-expected config/sync error
  - No need code for search data preparation except config

- MySQL
  - [Version](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-versions.html): 5.5, 5.6, 5.7, 8.0
  - Bulk operation
  - Simple nested sql: `insert into select`
  - Ignore `DuplicateKeyException`, not count as failure
  - **Low latency**
- Kafka
  - [Version](https://www.confluent.io/blog/upgrading-apache-kafka-clients-just-got-easier/): 0.10.0 or later
  - Bulk operation
  - Using `id` of data source as `key` of record, making sure the [orders between records](https://stackoverflow.com/questions/29511521/is-key-required-as-part-of-sending-messages-to-kafka)
  - Using `SyncResult` as msg `data`
  - Json serializer/deserializer (see [here](https://github.com/zzt93/syncer/issues/1) for future opt)
  - **Notice**: Kafka msg consumer has to handle change event idempotent;
  - **Notice**: May [in disorder](https://stackoverflow.com/questions/46127716/kafka-ordering-guarantees) if error happen;
  - Easy to re-consume, rebuild without affect biz db;
- HBase
  - [Version](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Compatibility.html#Wire_Protocols)
  
<a name="join_in_es">[1]</a>: Be careful about this feature, it may affect your performance

### Mis
- Http Endpoints
  - Port decision:
    - If no port config, `Syncer` will try ports between `[40000, 40010)`
    - If port is configured via either command line or env var `port` or `port` in `config.yml`
    syncer will use that port
    - If port is configured in multiple locations: command line, env var and config file, the precedence will be
      - command line option
      - env var
      - file config
  - `http://ip:port/health`: report `Syncer` status dynamically;

- JMX Endpoints
  - Use `jconsole` to connect to `Syncer`, you can [change the logging level](https://logback.qos.ch/manual/jmxConfig.html) dynamically; (Or change log level by `--debug` option when start)

  
### Limitation
- MySQL:
  - Don't change the numeric suffix naming of binlog, or it will fail the voting of binlog
  - Supported version: depend on this [binlog connector lib](https://github.com/shyiko/mysql-binlog-connector-java)
  - Not support composite primary key
  - Not support update primary key
  - Only support update/delete by query exact value, i.e. no support query analyzed field (`text` query when update)
  - Data of numeric types (tinyint, etc) always returned **signed** regardless of whether column definition includes "unsigned" keyword or not.
  You may need to convert to unsigned if necessary.
    - If your output is MySQL, Syncer will handle this situation for you in new binlog connector
  ```
     Byte.toUnsignedInt((byte)(int) fields['xx'])
     // or
     SyncUtil.unsignedByte(sync, "xx");
  ```
  - data of `*text`/`*blob` types always returned as a byte array (for `var*` this is true in future).
  You may need to convert to string if necessary.
    - If your output is MySQL, Syncer handle this situation for you.
  ```
    new String(fields['xx'])
    // or 
    SyncUtil.toStr(sync, "xx");
  ```
- Mongo:
  - Not delete field from ES if sync to ES
  - [Driver client compatibility](https://docs.mongodb.com/ecosystem/drivers/java/#mongodb-compatibility)
  - For version 4.0 and later (Use [change stream](https://docs.mongodb.com/manual/changeStreams/)):
    - Storage Engine: WiredTiger
    - Replica Set Protocol Version: The replica sets and sharded clusters must use replica set protocol version 1 (pv1).
    - [Read Concern “majority”](https://docs.mongodb.com/manual/reference/read-concern-majority/#readconcern.%22majority%22) Enabled.
  
- ES
  - Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause version conflict and fail the change
  - Update/Delete by query will be executed at once, i.e. will not be buffered or use batch
