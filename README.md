# Syncer: sync & manipulate data from MySQL/MongoDB to Elasticsearch/MySQL/Http Endpoint

## Features

### Consistency

#### Aims 

- Eventual Consistency: Make data reach destination
- Order Problem: Make data reach in same order as it is
  - update `item(id1)` set `field1` to `1`; then, update `item(id1)` set `field1` to `2`;
  - insert `item(id1)` with `field1` as `1`; delete `item(id1)`;

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

If you are changing the `id` of event, it always means you are doing joining like I do, which is no
way to make consistency promise because Syncer can only provide 'at least once' semantic

### Input -- DataSource

- Support listening to both MySQL & MongoDB & DRDS of Aliyun (https://www.aliyun.com/product/drds)
- MySQL master source filter:
  - Schema filter, support regex
  - Table name filter
  - Interested column filter
  - automatic primary key detection and set into `id`
  - If a table match multiple schema & table (because the usage of regex), an error message will be logged and
      syncer will use any one that match filter column
  - If an event go through column filter, and only primary key is left:
    - If event type is `UPDATE`, then discard this event -- because not support update id now;
    - Other event type, keep it.
  - Support reading from binlog file to do data recovering in case of loss of data (`syncer.producer.input.masters[x].file`)
  - Support specify binlog file/position to start reading (`input.masters[x].syncMeta`)
- MongoDB master source filter:
  - Version: 3.x
  - Schema filter, support regex
  - Collection name filter
  - automatic `_id` detection and set into `id`
  - If an event match multiple schema & table, we will use the first specific match to filter/output,
  i.e. the specific schema config will override the regex schema config
  - If an event go through column filter, and only primary key is left:
    - If event type is `UPDATE`, then discard this event -- because not support update id now;
    - Other event type, keep it.
- DRDS:
  - Same config as MySQL, but need to connect directly to RDS's MySQL because DRDS not support binlog dump
  - Remember to fetch partition key in `rowName`

- Remember where we leave last time by writing file/position of binlog/oplog, and resume from there so as to avoid any data loss
  - More than once (at-least-once): we can ensure the at least once semantics now, so you need to make sure your `SyncData`
  is idempotent and your destination can handle it. Counterexample: a table without primary key definitely
  can't handle it and cause duplicate data soon or later.
- Multiple consumer can share a common connection to same data source, i.e. MySQL/MongoDB, to reduce the
burden of remote master
- Automatically skip synced item for consumers according to register info 

---

After data items come out from `Input` module, it is converted to `SyncData`(s) -- the abstraction of
a single data change. In other words, a single binlog item may contain multiple line change and convert
to multiple `SyncData`s.

### Filter -- Operation

Manipulate `SyncData` via (for more details, see input part of *[Consumer Pipeline Config](#consumer_config)*):

- `if`
- `switcher`
- `foreach`
- all public method in `SyncData`:
  - `addRecord(String key, Object value)` 
  - `renameRecord(String oldKey, String newKey)` 
  - `removeRecord(String key)`
  - `removeRecords(String... keys)` 
  - `containRecord(String key)` 
  - `updateRecord(String key, Object value)` 
  - ...
  - `syncByQuery()`
  - `extraQuery(String schemaName, String tableName)`
- all data field in `SyncData`:
  - `schema`
  - `table`
  - `id`
  - `records`
  - `extra`

### Output -- DataSink

- If output channel meet too many failure/error (exceeds `countLimit`), it will abort and change health to `red` 
- Elasticsearch
  - Version: 5.x
  - Bulk operation
  - Update/Delete documents by `UpdateByQuery` or `DeleteByQuery`
  - Join/merge documents from different source when push to ES<sup>[1](#join_in_es)</sup>
    - ExtraQuery: do extra query to fetch extra needed info
      - Support multiple extra dependent query via special mark `$var$`
    - One to many relationship (parent-child relationship in ES)for document in different index
    - Self referential relationship handle
  - Add `upsert` support, fix `DocumentMissingException` use `upsert`

- Http Endpoint
- MySQL
  - Bulk operation
  - Simple nested sql: `insert into select`
  - Ignore `DuplicateKeyException`, not count as failure
- Kafka
  - Bulk operation
  
  
<a name="join_in_es">[1]</a>: Be careful about this feature, it may affect your performance

### Mis
- Http Endpoints
  - Port decision:
    - If no port config, `Syncer` will try ports between `[40000, 40010)`
    - If port is configured via either command line `port` or `syncer.port` in `config.yml`
    syncer will use that port
    - If port is configured both in command line and config file, command line option will override file config
  - `http://ip:port/health`: report `Syncer` status dynamically;

- JMX Endpoints
  - Use `jconsole` to connect to `Syncer`, you can [change the logging level](https://logback.qos.ch/manual/jmxConfig.html) dynamically;

### Limitation
- MySQL:
  - Supported version: depend on this [binlog connector lib](https://github.com/shyiko/mysql-binlog-connector-java)
  - Not support composite primary key
  - Not support update primary key
  - Only support update/delete by query exact value, i.e. no support query analyzed field (`text` query when update)

### Notice

- Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause version conflict and fail the change
- Update/Delete by query will be executed at once, i.e. will not be buffered or use batch
- Don't change the numeric suffix naming of binlog, or it will fail the voting of binlog

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
- `syncer.producer.input.masters[x]`
 - `type`: MySQL, Mongo
 - <a name="connection"></a>`connection`: `ip`, `address`, `port`, `user`, `password`, `passwordFile`
 - `file`: absolute path to binlog file

```yml

syncer.producer.input:
  masters:
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
- `input.master[x]`:
 - `type`: same as producer
 - `connection`: [same as producer](#connection)
 - `syncMeta`:
   - `binlogFilename`: string name of remote master's binlog file name
   - `binlogPosition`: position you want to start listening
 - `schemas[x]`:
   - `name`: schema name, allow regex
   - `tables[x]`:
     - `name`: entity name
     - `rowName`: entity fields list
 - `scheduler`:


#### Filter

This part is implemented by [Spring EL](https://docs.spring.io/spring/docs/5.0.0.M5/spring-framework-reference/html/expressions.html), i.e. you can use any syntax Spring EL supported
if I didn't listed.

- `statement`: list of String code to be executed.
  - e.g.
  ```yml
    - statement: ["#type=table", "isWrite()"]
  ```
- `switcher`
  - support `default` case
  - only execute one case
  - e.g.
  ```yml
    - switcher:
        switch: "table"
        case:
          "file": ["#docType='plain'", "renameRecord('uploader_id', 'uploaderId').renameRecord('parent_id', 'parentId')"]
          "user": ["#suffix='' ", "renameRecord('superid', 'superId')"]

  ```
- `foreach`: in most cases, you can use [Spring EL's collection projection](https://docs.spring.io/spring/docs/5.0.0.M5/spring-framework-reference/html/expressions.html#expressions-collection-projection) rather than this feature
- `if`
  - `create`: create a new event (or a bunch) and cp value & execute post creation statement
  - `drop`: drop this event
  - `statement`: same with outer `statement`
  - `switcher`: same as above
  - `foreach`
  ```yml
    - if:
        condition: "table == 'user' && isUpdate()"
        ifBody:
          - create:
              copy: ["id", "table", "#suffix", "#title", "#docType"]
              postCreation: ["addRecord('ownerTitle', #title)", "syncByQuery().filter('ownerId', id)", "id = null"]
        elseBody:
          - drop: {}
  ```
- all public method in `SyncData`:
  - `isWrite()`
  - `isUpdate()`
  - `isDelete()`
  - `toWrite()`
  - `toUpdate()`
  - `toDelete()`
  - `getRecordValue(String key)`
  - `addExtra(String key, Object value)`
  - `addRecord(String key, Object value)`
  - `renameRecord(String oldKey, String newKey)`
  - `removeRecord(String key)`
  - `removeRecords(String... keys)` 
  - `containRecord(String key)` 
  - `updateRecord(String key, Object value)` 
  - `syncByQuery()`: update/delete by query, supported by ES/MySQL output channel
    - `SyncByQueryES`
  - `extraQuery(String schemaName, String tableName)`: usually work with `create` to convert one event to multiple events
    - `ExtraQuery`: enhance syncer with multiple dependent query;
- all data field in `SyncData`:
  - `schema`: schema/db/index
  - `table`: table or collection
  - `id`: data primary key or similar thing
  - `records`: data content of this sync event converted from log content according to your `schema` config
  **Notice**:
    - if your interested column config (`rowName`) has name of `primary key`, records will have it. Otherwise, it will only in `id` field;
  - `extra`: an extra map to store extra info

#### Output

- Special expression to do output mapping:
  - "records.*": map.put('your_key', `records`)
  - "records.*.flatten": map.putAll(records)
  - "extra.*": map.put('your_key', `extra`)
  - "extra.*.flatten": map.putAll(`extra`)
- `batch`: support output change in batch
  - `size`: flush if reach this size
  - `delay`: flush if every this time in `MILLISECONDS`
  - `maxRetry`: max retry if met error
- `failureLog`: failure log config
  - `countLimit`: failure
  - `timeLimit`: failure log item in this time range
- `requestMapping`, `rowMapping`: how to convert `SyncData` to suitable format
and send to where
- `elasticsearch`
  - When using this channel, you may prefer to not include `id` like field in interested column config (`rowName`),
  because it is always no need to include it in data field for ES and we will auto detect it and set it for you.
  - e.g.
  ```yml
    elasticsearch:
      connection:
        clusterName: test-${ACTIVE_PROFILE}
        clusterNodes: ["${HOST_ADDRESS}:9300"]
      requestMapping: # mapping from input data to es request
        enableExtraQuery: true
        retryOnUpdateConflict: 3
        upsert: false
        index: "table + #suffix" # default: schema
        type: "#docType" # default: table
        documentId: "id" # default: id
        fieldsMapping: # default: records.*.flatten
          "records": "records.*.flatten"
      batch:
        size: 100
        delay: 1000
        maxRetry: 5
      refreshInMillis: 1000
      failureLog:
        countLimit: 1000

  ```
- `mysql`
  - Use
  - e.g.:
  ```yml
    mysql:
      connection:
        address: ${HOST_ADDRESS}
        port: 3306
        user: xxx
        password: xxx
      rowMapping:
        schema: "'test'"
        table: "table"
        id: "id"
        rows:
          "records": "records.*.flatten"
      batch:
        size: 100
        delay: 100
        maxRetry: 5
      failureLog:
        countLimit: 1000
  ```
- Http endpoint

#### In All
More samples can be found under `src/test/resource/`
```yml
version: 1.1

consumerId: todomsg

input:
  masters:
    - connection:
        address: ${HOST_ADDRESS}
        port: 27017
      type: Mongo
      scheduler: direct
      schemas:
        - name: "test"
          tables:
          - name: test
            rowName: [createTime, content]
    - connection:
        address: ${HOST_ADDRESS}
        port: 3306
      scheduler: mod
      schemas:
        - name: "test_${ACTIVE_PROFILE}.*"
          tables:
          - name: user
            rowName: [user_id, title]
          - name: addr
            rowName: [address]
        - name: "file_${ACTIVE_PROFILE}.*"
          tables:
          - name: file
            rowName: [name]



# input result class: com.github.zzt93.syncer.common.data.SyncData

filter:
  - switcher:
      switch: "table"
      case:
        "user": ["renameRecord('xxx', 'yyy')"]
  - if:
      condition: "table == 'user' && isUpdate()"
      ifBody:
        - create:
            copy: ["id", "table", "#suffix", "#title", "#docType"]
            postCreation: ["addRecord('ownerTitle', #title)", "syncByQuery().filter('ownerId', id)", "id = null"]
      elseBody:
        - drop: {}



# filter result class: com.github.zzt93.syncer.common.data.SyncData

output:
  mysql:
    connection:
      address: ${HOST_ADDRESS}
      port: 3306
      user: root 
      passwordFile: mysql-password
    rowMapping:
      schema: "schema"
      table: "table"
      id: "id"
      rows:
        "records": "records.*.flatten"
    batch:
      size: 100
      delay: 100
      maxRetry: 5
```

### Syncer Config

```yml
syncer:
  # can be overrided via command line args `server.port`
  port: 12345
  ack:
    flushPeriod: 100
  input:
    input-meta:
      last-run-metadata-dir: /data/syncer/input/last_position/

  filter:
    worker: 6

  output:
    worker: 2
    batch:
      worker: 2
    output-meta:
      failure-log-dir: /data/syncer/output/failure/

```
### Run
```
git clone https://github.com/zzt93/syncer
mvn package
# /path/to/config/: producer.yml, consumer.yml, password-file
# use `-XX:+UseParallelOldGC` if you have less memory and lower input pressure
# use `-XX:+UseG1GC` if you have at least 4g memory and event input rate larger than 2*10^4/s
java -server -XX:+UseG1GC -jar syncer.jar [--port=9999] [--config=/absolute/path/to/syncerConfig.yml] --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
```

## Test
### Correctness Test
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
- Throughput: limited by filter worker number, in average 2000 events per worker
- CPU: 80-90
- Memory: 4g
- IO
  - Network
  - Disk
- JVM
  - Thread
  - Lock contention 

### Used In Production
- For search data sync
- For auth data sync
- For data recovery: In case of drop table mistakenly, or you know where to start & end

## TODO
[See Issue 1](https://github.com/zzt93/syncer/issues/1)

---

## Implementation

- [Elasticsearch & MySQL Sync Challenge(1)](https://tonyz93.blogspot.com/2017/11/elasticsearch-mysql-sync-challenge-1.html)
- [Elasticsearch & MySQL Sync Challenge(2): Event Driven](https://tonyz93.blogspot.com/2017/12/elasticsearch-mysql-sync-challenge-2.html)
- [Elasticsearch & MySQL Sync Challenge(3): Implementation](https://tonyz93.blogspot.com/2017/12/elasticsearch-mysql-sync-challenge-3.html)
- [Elasticsearch & MySQL Sync Challenge(4): Quality Attributes](https://tonyz93.blogspot.com/2018/01/elasticsearch-mysql-sync-challenge-4.html)
- [Elasticsearch & MySQL Sync Challenge(5): Redesign](https://tonyz93.blogspot.com/2018/02/elasticsearch-mysql-sync-challenge-5.html)

### Input Module

- Load yml file into environment property source
- Bind property source into config model class

#### Problem & Notice

- For collection field: getter and setter will all be invoked -- this behavior depends on Spring Boot
  - If only invoke getter?
  - For list and map field, has different behavior:
    - list: `list = getList(); list.clear(); list.add(xxx); setList(list);`
    - map: `map = getMap(); map.add(xxx); setMap(map);`

### Output Module

#### Json Mapper
- For now, mapping document value using `toString()`: {@link XContentBuilder#unknownValue}
  - java.sql.Timestamp format: 'yyyy-MM-dd HH:mm:ss.SSS'. For now, if you need other format, you have to format it to string by yourself
  - Maybe change to jackson

---

## How to ?

If you have any problems with how to use `Syncer` or bugs of it, write a issue.
I will handle it as soon as I can.