
# Syncer: sync & manipulate data from MySQL/MongoDB to Elasticsearch/MySQL/Http/Kafka Endpoint

## Features

### Consistency

#### Aims 

- Eventual Consistency: Make data reach destination
- Order Problem: Make data reach in same order as it is
  - update `item(id1)` set `field1` to `1`; then, update `item(id1)` set `field1` to `2`;
      ```sql
    update t set name='a' where id = 1;
    update t set name='b' where id = 1;
      ```
  - insert `item(id1)` with `field1` as `1`; delete `item(id1)`;
      ```sql
    insert into t values (1);
    delete from t where id = 1;
      ```
#### Consistency Promise

- Master slave replication protocol: If network has problem, MySQL master will re-send lost packet
- `WAL`: Consumer module adopts `write ahead log`, write what receive then try to process & send
- Checkpoint: Consumer module remember where we leave, try to not miss data if syncer shutdown in accident
- Retry: If output channel fail to send to output target, retry until success or write to failure log
- Failure Log: If retry exceed configured num, write item to failure log for human recheck
- Event Scheduler: to solve *Order Problem* between events which has unchanged primary key
  - `mod`: `mod` integral primary key to make same row change always handled in order;
  - `hash`: hash primary key of data row, then `mod` hash value to schedule -- default value now;
  - `direct`: 
    - If your data source has only insert operation, you can choose this scheduler, which is faster;
    - *No order promise* for data source with insert/update/delete, higher output rate if you can endure some inconsistency;

---

If you are changing the `id` of event **but not call `SyncData.setPartitionField(x)`**, it always means you are doing joining like I do, which 
 - may fail consistency promise because the order between events may not scheduled as it should be;
 - may cause dup item because Syncer only make sure `exactly once semantic`;

### Updated Asynchronously
The business database query request is delayed as little as possible.
 
### Input -- DataSource

- Support listening to both MySQL & MongoDB & DRDS of Aliyun (https://www.aliyun.com/product/drds)
- If Syncer fail to connect to input data source, will abort
- MySQL master source filter:
  - Schema filter (naming as `repos`), support regex
  - Table name filter
  - Interested column filter
  - In a `UPDATE`, all interested column will be received even no change (different from `MongoDB`)
  - automatic primary key detection and set into `id`
  - If a table match multiple schema & table (because the usage of regex), an error message will be logged and
      syncer will use any one that match filter column
  - If an event go through column filter, and only primary key is left:
    - If event type is `UPDATE`, then discard this event -- because not support update id now;
    - Other event type, keep it.
  - Support reading from binlog file to do data recovering in case of loss of data (`input.masters[x].file`)
  - Support specify binlog file/position to start reading (`input.masters[x].connection.syncMeta[]`)
- MongoDB master source filter:
  - Version: 3.x, 4.0
    - Only 4.0 support field removed detection and sync (Because the limitation of ES/MySQL, it always means setting field to null in output target which may not what you want) 
  - Database filter (naming as `repos`), support regex
  - Collection name filter
  - In a `UPDATE`, only changed column will be received (different from `MySQL`)
  - automatic `_id` detection and set into `id`
  - If an event match multiple schema & table, we will use the first specific match to filter/output,
  i.e. the specific `repo` config will override the regex `repo` config
  - If an event go through column filter, and only primary key is left:
    - If event type is `UPDATE`, then discard this event -- because not support update id now;
    - Other event type, keep it.
  - If config user/password for auth, it should have permission of `[listDatabases, find]`
  - Only support listening first level field (Because MongoDB store json, it may have multiple levels)
- DRDS:
  - Same config as MySQL, but need to connect directly to RDS's MySQL because DRDS not support binlog dump
  - Remember to fetch partition key in `fields`

- Remember where we leave last time by writing file/position of binlog/oplog, and resume from there so as to avoid any data loss
  - More than once (at-least-once): we can ensure the at least once semantics now, so you need to make sure your output channel (the `consumer` of syncer output)
  is **idempotent** and your destination can handle it without dup. Counterexample: a table without primary key definitely
  can't handle it and cause duplicate data soon or later.
- Multiple consumer can share a common connection to same data source, i.e. MySQL/MongoDB, to reduce the
burden of remote master
- Automatically skip synced item for consumers according to register info 


The readConcern option allows you to control the consistency and isolation properties of the data read from replica sets and replica set shards.
Through the effective use of write concerns and read concerns, you can adjust the level of consistency and availability guarantees as appropriate, such as waiting for stronger consistency guarantees, or loosening consistency requirements to provide higher availability.
MongoDB drivers updated for MongoDB 3.2 or later support specifying read concern.

---

After data items come out from `Input` module, it is converted to `SyncData`(s) -- the abstraction of
a single data change. In other words, a single binlog item may contain multiple line change and convert
to multiple `SyncData`s.

### Filter -- Operation

Manipulate `SyncData` via (for more details, see input part of *[Consumer Pipeline Config](#consumer_config)*):

- `method`: write a java method to handle `SyncData`
  - Global variable:
    - `logger` to do logging
  - Already imported (**May add more in future**):
    - `java.util.*`
    - `org.slf4j.Logger`
    - `com.github.zzt93.syncer.data.*`
    - `com.github.zzt93.syncer.data.util.*`
    - Use full class name if you need other class, like `java.util.function.Function`


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
  - **Notice**: Kafka msg consumer has to handle event idempotent;
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

- Shutdown process
  - Producer starter shutdown
    - Connector shutdown
    - Starter service shutdown
  - Consumer starter shutdown
    - Output stater shutdown
      - Output channel shutdown
      - Batch service shutdown
    - Filter-output service shutdown
  
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

## Use Syncer

### Preparation

- MySQL config
  - binlog_format: row
  - binlog_row_image: full
- MongoDB config:
  - (optional) update `bind_ip` to allow listens for connections from applications on configured addresses.
  - start with enable replication set:
    - `mongod --replSet myapp`
    - Or use docker: `docker run -d --name mongodb -p 27017:27017 -v /root/mongodb-container/db:/data/db mongo:3.2 mongod --replSet chat`
  - init replication set in shell: `rs.initiate()`

### Producer Data Source Config
- `input[]`
 - `type`: MySQL, Mongo
 - <a name="connection"></a>`connection`: `ip`, `address`, `port`, `user`, `password`, `passwordFile`
 - `file`: absolute path to binlog file

```yml

input:
- connection:
    address: ${HOST_ADDRESS}
    port: 3306
    user: xxx
    password: yyy

- connection:
    address: ${HOST_ADDRESS}
    port: 27018
  type: mongo
```
### <a name="consumer_config"></a>Consumer Pipeline Config

#### Input
- `input[]`:
  - `type`: same as producer
  - `connection`: [same as producer](#connection)
  - `syncMeta`:
    - `binlogFilename`: string name of remote master's binlog file name
    - `binlogPosition`: position you want to start listening
  - `repos[x]`:
    - `name`: repo name, allow regex
    - `entities[x]`:
      - `name`: entity name
      - `fields`: entity fields list, can omit it which represents all fields
  - `scheduler`:
    - `mod`: `mod` integral primary key to make same row change always handled in order;
    - `hash`: hash primary key of data row, then `mod` hash value to schedule -- default value now;
    - `direct`: 
      - If your data source has only insert operation, you can choose this scheduler, which is faster;
      - *No order promise* for data source with insert/update/delete, higher output rate if you can endure some inconsistency;
  - `onlyUpdated`: whether sync not `updated` event (only for `MySQL`)
    - `updated` definition: `Objects.deepEquals` == true 

```yml
input:
  - connection:
      clusterNodes: [${MYSQL_ADDR}]
    repos:
      - name: "test_0"
        entities:
          - name: correctness
          - name: types
          - name: news
      - name: "simple_0"
        entities:
          - name: simple_type
```
#### Filter


- `method` (**preferred: more powerful and easier to write**) : write a java class implements `MethodFilter`  to handle `SyncData`
  - Import dependency:
  ```xml
        <dependency>
            <groupId>com.github.zzt93</groupId>
            <artifactId>syncer-data</artifactId>
            <version>1.0.1-SNAPSHOT</version>
        </dependency>

  ```
  - Write a class implement `MethodFilter`
  ```java
    public class MenkorFilterImpl implements MethodFilter {
      @Override
      public void filter(List<SyncData> list) {
        SyncData data = list.get(0);
        if (data.getField("location") != null) {
          Map location = SyncUtil.fromJson((String) data.getField("location"));
          if (!location.isEmpty()) {
            data.addField("geom", SQLFunction.geomfromtext("point(" + location.get("longitude") + "," + location.get("latitude") + ")"));
          }
        }
      }
    }

  ```
  - Copy method filter to config file:
  ```$xslt
  filter:
    - method: '      public void filter(List<SyncData> list) {
                       SyncData data = list.get(0);
                       if (data.getField("location") != null) {
                         Map location = SyncUtil.fromJson((String) data.getField("location"));
                         if (!location.isEmpty()) {
                           data.addField("geom", SQLFunction.geomfromtext("point(" + location.get("longitude") + "," + location.get("latitude") + ")"));
                         }
                       }
                     }'
                     
  ```
  - Limitation: 
    - Not support Single Line Comments or Slash-slash Comments



For more config, see [doc/filter-el.md](doc/filter-el.md)

#### Output


- `elasticsearch`
  - When using this channel, you may prefer to not include `id` like field in interested column config (`fields`),
  because it is always no need to include it in data field for ES and we will auto detect it and set it for you.
  - e.g.
  ```yml
  elasticsearch:
    connection:
      clusterName: ${ES_CLUSTER}
      clusterNodes: ["${ES_ADDR}:9300"]

  ```
- `mysql`
  - e.g.:
  ```yml
  mysql:
    connection:
      address: ${MYSQL_OUT}
      port: 3306
      user: root
      password: ${MYSQL_OUT_PASS}
  ```
For more config, see [doc/output.md](doc/output.md)

#### In All
```yaml
version: 1.3

consumerId: simplest


input:
  - connection:
      clusterNodes: [${MYSQL_ADDR}]
    repos:
      - name: "test_0"
        entities:
          - name: correctness
          - name: types
          - name: news
      - name: "simple_0"
        entities:
          - name: simple_type

# tmp solution for unsigned byte conversion, no need if no unsigned byte
filter:
  - method: '
  public void filter(List<SyncData> list) {
     SyncData sync = list.get(0);
     SyncUtil.unsignedByte(sync, "tinyint");
     SyncUtil.unsignedByte(sync, "type");
  }
'
# tmp solution

output:
  mysql:
    connection:
      address: ${MYSQL_OUT}
      port: 3306
      user: root
      password: ${MYSQL_OUT_PASS}
  elasticsearch:
    connection:
      clusterName: ${ES_CLUSTER}
      clusterNodes: ["${ES_ADDR}:9300"]
```
Full and usable samples can be found under [`test/config/`](test/config/)

### Syncer Config

Usually no need to care, because it is used for meta info of Syncer. Samples can be found in [resources](syncer-core/src/main/resources)

### Run
```
git clone https://github.com/zzt93/syncer
cd syncer/ && mvn package
# /path/to/config/: producer.yml, consumer.yml, password-file
# use `-XX:+UseParallelOldGC` if you have less memory and lower input pressure
# use `-XX:+UseG1GC` if you have at least 4g memory and event input rate larger than 2*10^4/s
java -server -XX:+UseG1GC -jar ./syncer-core/target/syncer-core-1.0-SNAPSHOT.jar [--debug] [--port=40000] [--config=/absolute/path/to/syncerConfig.yml] --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
```

## Test
### Dependency
- [Docker](https://docs.docker.com/install/#server)
  - [vm.max_map_count](https://stackoverflow.com/questions/41192680/update-max-map-count-for-elasticsearch-docker-container-mac-host) may be need to
  be set for some os for ES docker image to run
- [Docker compose](https://docs.docker.com/compose/install/)

### Integration Test
#### Test data: 
  - size: 7M
  - machines: 3
  - databases: 3 in logic, after horizontal split is 24
  - tables: 90+ for each database; listening: 5 for each database
  - types: bigint, varchar, text, tinyint, timestamp, smallint, int, unsigned, longtext
#### How
- Insert/load data, count in mysql & es and compare numbers;
- Delete data, count in mysql & es and compare numbers;

### Pressure Test
- 10G & 10^8 lines
  - load every 10^5 lines by `mysqlimport`
  - no pause between import
- Throughput
  - MySQL output: 1300+ insert/s
  ```bash
    time: 20190407-022652
    src=800000
    dst=9302
    time: 20190407-022654
    src=800000
    dst=12070
    time: 20190407-022656
    src=800000
    dst=14863
    time: 20190407-022658
    src=800000
    dst=17536
  ```
  - ES output: 10000+ insert/s
  ```bash
    time: 20190406-083524
    src=800000
    dst=79441
    time: 20190406-083527
    src=800000
    dst=130193
    time: 20190406-083530
    src=800000
    dst=134752
    time: 20190406-083533
    src=800000
    dst=190517
  ```
- CPU: 80-90
- Memory: 4g
  - Increase batch size & flush period, increase performance in cost of higher memory usage (only for ES)
- IO
  - Network
  - Disk
- JVM
  - Thread
  - Lock contention 

### Used In Production
- Search system: search data sync
- Micro-service: auth/recommend/chat data sync
  - Sync Requirement: low latency, high availability
- Join table: avoid join in production env, use space for speed by joining table
  - Sync Requirement: low latency, high availability
- Kafka: sync data to kafka, for other heterogeneous system to use
- For data recovery: In case of drop entity mistakenly, or you know where to start & end
- For alter table sync: 
  - [MySQL very slow for alter table](https://stackoverflow.com/questions/12774709/mysql-very-slow-for-alter-table-query)
  - [MySQL 8.0: InnoDB now supports Instant ADD COLUMN](https://mysqlserverteam.com/mysql-8-0-innodb-now-supports-instant-add-column/)

## TODO
[See Issue 1](https://github.com/zzt93/syncer/issues/1)

---

## Implementation
Implementation detail can be found in [doc/impl.md](doc/impl.md)

## Config File Upgrade Guide

### From 1.1 to 1.2

- Replace in case sensitive
  - "schemas" -> "repos"
  - "tables" -> "entities"
  - "rowName" -> "fields"
  - "Record" -> "Field"
  - "records" -> "fields"

## How to ?

If you have any problems with how to use `Syncer` or bugs of it, write a issue.
I will handle it as soon as I can.

## FAQ

- Q: "Got error produce response in correlation id xxx on topic-partition xxx.xxPartition-0, splitting and retrying (5 attempts left). Error: MESSAGE_TOO_LARGE"?
  - A: Adjust message `batch.size` to smaller number or config `kafka` to receive large message