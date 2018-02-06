# Syncer: sync & manipulate data from MySQL/MongoDB to Elasticsearch/MySQL/Http Endpoint

## Features

### Consistency

#### Consistent mode: 
  - `strict`: make sure output is successful, otherwise, retry until success
  - `loose`: try to send to output

#### Consistency Promise

- If network has problem, MySQL master slave protocol?
- Input module remember where we leave, try to not miss data if syncer shutdown in accident
- If data is still not send in output channel when input is starting to handle next event, and syncer shutdown?
- If output channel fail to send to output target, retry until success


### Input

- Support listening to both MySQL & MongoDB
- MySQL master source filter:
  - Schema filter, support regex
  - Table name filter
  - Interested column filter
  - automatic primary key detection and set into `id`
  - If an event match multiple schema & table, use which filter is undefined
- MongoDB master source filter:
  - Version: 3.x
  - Schema filter, support regex
  - Collection name filter
  - automatic `_id` detection and set into `id`
  - If an event match multiple schema & table, we will use the first specific match to filter/output,
  i.e. the specific schema config will override the regex schema config
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
  - `insertByQuery(String schemaName, String tableName)`
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
  - Default exclude `primary key` of a row/document from the json body of ES request

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
  - Not support composite primary key
  - Not support update primary key
  - Only support update/delete by query exact value, i.e. no support query analyzed field (`text` query when update)

## Notice

- Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause version conflict and fail the change
- Update/Delete by query will be executed at once, i.e. will not be buffered or use batch


## Producer Data Source Config
## TODO
- Support set parent of ES
- Row image format support?
  - Add must appeared field restriction -- now only primary key
  - Opt: keep only changed field in update event & primary key in delete event -- include must appear field
- Join by query mysql?
- Make all output channel strict

- MySQL master connection
- Mongo master connection

## Consumer Pipeline Config

### Input
- MySQL master connection: specify the master want to listen to
- Mongo master connection: specify the master want to listen to
### Filter

- statement: implemented by Spring EL, can call all public method in `SyncData`
- switcher
  - support `default` case
  - only execute one case
- foreach: in most cases, you can use [Spring EL's collection projection](https://docs.spring.io/spring/docs/3.0.x/reference/expressions.html) rather than this feature
- if
  - clone: clone a new event
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
  - `insertByQuery(String schemaName, String tableName)`: usually work with `clone` & `dup` to convert one event to multiple events
    - `ExtraQuery`
- all data field in `SyncData`:
  - `schema`: schema/db/index
  - `table`: table or collection
  - `id`: data primary key or similar thing
  - `records`: data content of this sync event converted from log content
  - `extra`

### Output Choice

 - Elasticsearch
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
            copyValue: ["id"]
            new:
            - ["table='permission_identity'", "addRecord('name', '盟主')
            .addRecord('permission_category', '[105,205,305,405,505,605,705,805]')
            .addRecord('permission_identity_type', 5)
            .addRecord('allianceId', id)" ]
            - ["table='role_permission'", "addRecord('role_id', records['owner_role_id'])
            .addRecord('affair_id', records['root_affair_id'])
            .addRecord('alliance_id', records['id'])",
            "insertByQuery(schema, 'permission_identity').filter('is_super', 1).filter('allianceId', records['id'])
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
java -server -XX:+UseParallelGC -jar syncer.jar --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
```

## Test
### Stress Test
- 10G
- CPU
- Memory
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
- Support set parent of ES
- Row image format support?
  - Add must appeared field restriction -- now only primary key
  - Opt: keep only changed field in update event & primary key in delete event -- include must appear field
- Join by query mysql?

---

## Implementation

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