# Syncer: sync & manipulate data from MySQL/MongoDB to Elasticsearch/MySQL/Http Endpoint

## Features

### Consistency

#### Aims 

- Eventual Consistency: Make data reach destination
- Order Problem: Make data reach in same order as it is
  - update id1 set 1; update id1 set 2;
  - insert id1 1; delete id1;

#### Consistency Promise

- Master slave replication protocol: If network has problem, MySQL master will re-send lost packet
- `WAL`: Consumer module adopts `write ahead log`, write what receive then try to process & send
- Checkpoint: Consumer module remember where we leave, try to not miss data if syncer shutdown in accident
- Retry: If output channel fail to send to output target, retry until success or write to failure log
- Failure Log: If retry exceed configured num, write item to failure log for human recheck
- Event Scheduler: to solve *Order problem* between events
  - `mod`: `mod` integral primary key to make same row change always handled in order;
  - `hash`: hash primary key of data row, then `mod` hash value to schedule -- default value now;
  - `direct`: 
    - If your datasource has only insert operation, you can choose this scheduler, which is faster;
    - *No order promise* for datasource with insert/update/delete, higher output rate if you can endure some inconsistency;


### Input

- Support listening to both MySQL & MongoDB
- MySQL master source filter:
  - Schema filter, support regex
  - Table name filter
  - Interested column filter
  - automatic primary key detection and set into `id`
  - If a table match multiple schema & table (because the usage of regex), a exception will be thrown
  - If an event go through column filter, and only primary key is left:
    - If event type is UPDATE_ROWS, then discard this event -- because not support update id now;
    - Other event type, keep it.
  - Support reading from binlog file to do data recovering in case of loss of data 
- MongoDB master source filter:
  - Version: 3.x
  - Schema filter, support regex
  - Collection name filter
  - automatic `_id` detection and set into `id`
  - If an event match multiple schema & table, we will use the first specific match to filter/output,
  i.e. the specific schema config will override the regex schema config
  - If an event go through column filter, and only primary key is left:
    - If event type is `UPDATE_ROWS`, then discard this event -- because not support update id now;
    - Other event type, keep it.
- Remember start file/position of binlog/oplog, and resume from where we leave so as to avoid any data loss
  - More than once: we can ensure the at least once semantics now, so you need to make sure your `SyncData`
  is idempotent and your destination can handle it. Counterexample: a table without primary key definitely
  can't handle it and cause duplicate data soon or later.
- Multiple consumer can share a common connection to same data source, i.e. MySQL/MongoDB


### Filter

Manipulate `SyncData` through (for more details, see input part of *Pipelinie Config*):

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

### Output
- Elasticsearch
  - Version: 5.x
  - Bulk operation
  - Update/Delete documents by `UpdateByQuery` or `DeleteByQuery`
  - Join/merge documents from different source when push to ES<sup>[1](#join_in_es)</sup>
    - One to many relationship (parent-child relationship in ES)for document in different index
    - Self referential relationship handle

- Http Endpoint
- MySQL
 - Bulk operation
 - Simple nested sql: `insert into select`

<a name="join_in_es">[1]</a>: Be careful about this feature, it may affect your performance

## Usage 

- MySQL config
  - binlog_format: row
  - binlog_row_image: full
- MongoDB config:
  - (optional) update `bind_ip` to allow listens for connections from applications on configured addresses.
  - start with enable replication set: 
    - `mongod --replSet myapp`
    - Or use docker: `docker run -d --name mongodb -p 27017:27017 -v /root/mongodb-container/db:/data/db mongo:3.2 mongod --replSet chat`
  - init replication set in shell: `rs.initiate()`

### Limitation
- MySQL:
  - Supported version: depend on this [binlog connector lib](https://github.com/shyiko/mysql-binlog-connector-java)
  - Not support composite primary key
  - Not support update primary key
  - Only support update/delete by query exact value, i.e. no support query analyzed field (`text` query when update)

## Notice

- Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause version conflict and fail the change
- Update/Delete by query will be executed at once, i.e. will not be buffered or use batch


## Producer Data Source Config
- MySQL master connection
- Mongo master connection

## Consumer Pipeline Config

### Input
- MySQL master connection: specify the master wanting to listen to
- Mongo master connection: specify the master wanting to listen to
### Filter

- statement: implemented by Spring EL, can call all public method in `SyncData`
- switcher
  - support `default` case
  - only execute one case
- foreach: in most cases, you can use [Spring EL's collection projection](https://docs.spring.io/spring/docs/3.0.x/reference/expressions.html) rather than this feature
- if
  - new: create a new event (or a bunch) and cp value & execute statement
  - drop
  - statement: same with outer `statement`
  - dup: duplicate multiple event
  - switch
  - foreach
- all public method in `SyncData`:
  - `addRecord(String key, Object value)` 
  - `renameRecord(String oldKey, String newKey)` 
  - `removeRecord(String key)`
  - `removeRecords(String... keys)` 
  - `containRecord(String key)` 
  - `updateRecord(String key, Object value)` 
  - ...
  - `syncByQuery()`: update/delete by query, now only support ES
    - `SyncByQueryES`
  - `extraQuery(String schemaName, String tableName)`: usually work with `new` & `dup` to convert one event to multiple events
    - `ExtraQuery`: enhance syncer with multiple dependent query;
- all data field in `SyncData`:
  - `schema`: schema/db/index
  - `table`: table or collection
  - `id`: data primary key or similar thing
  - `records`: data content of this sync event converted from log content according to your config. 
  **Notice**: 
    - if your interested column config (`rowName`) has name of `id`, records will have it. Otherwise, it will only in `id` field;
  - `extra`: an extra map to store extra info

### Output Choice

 - Elasticsearch
  - When using this channel, you may prefer to not include `id` like field in interested column config (`rowName`),
  because it is always no need to include it in data field for ES and we will auto detect it and set it for you.
 - MySQL
 - Http endpoint

### Sample
More samples can be found under `src/test/resource/`
```yml

input:
  masters:
    - connection:
        address: ${HOST_ADDRESS}
        port: 27017
        user: root
        passwordFile: password
      type: mongo
      schemas:
        - name: "test_${ACTIVE_PROFILE}.*"
          tables:
          - name: user 
            rowName: [id, name, email]
        - name: "file_${ACTIVE_PROFILE}.*"
          tables:
          - name: file
            rowName: [id, uploader, public_type, state]
          - name: folder
            rowName: [id, uploader, public_type, state]



# input result class: com.github.zzt93.syncer.common.data.SyncData

filter:
  - switcher:
      switch: "table"
      case:
        "user": ["renameRecord('xxx', 'yyy')"]
  - if:
      condition: "table == 'test_table' && action == 'WRITE_ROWS'"
      ifBody:
        - dup:
            copy: ["id"]
            new:
            - ["table='permission_identity'", "addRecord('name', '盟主')
            .addRecord('permission_category', '[105,205,305,405,505,605,705,805]')
            .addRecord('permission_identity_type', 5)
            .addRecord('allianceId', id)" ]
            - ["table='role_permission'", "addRecord('role_id', records['owner_role_id'])
            .addRecord('affair_id', records['root_affair_id'])
            .addRecord('alliance_id', records['id'])",
            "extraQuery(schema, 'permission_identity').filter('is_super', 1).filter('allianceId', records['id'])
            .select('id').addRecord('identity_id')"]


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
## Run
```
git clone https://github.com/zzt93/syncer
mvn package
# /path/to/config/: producer.yml, consumer.yml, password-file
# use `-XX:+UseParallelOldGC` if you have less memory and lower input pressure
# use `-XX:+UseG1GC` if you have at least 4g memory and event input rate larger than 2*10^4/s
java -server -XX:+UseG1GC -jar syncer.jar --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
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