
# Syncer: MySQL/MongoDB => Elasticsearch/MySQL/Kafka/HBase 

## Features

 - Sync data consistently: make sure data consistency
 - Sync data async: little effect on origin server
 - Sync data and manipulate: write Java code to [customize sync](doc/detail-feature.md)
 - Sync data from [MySQL/MongoDB to Kafka/ES/MySQL/HBase](doc/detail-feature.md)
 - ETL and real-time data sync combined: missing no data

---

#### Consistency Aims 

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
#### Consistency Impl

- Master slave replication protocol: If network has problem, MySQL master will re-send lost packet
- `WAL`: Consumer module adopts `write ahead log`, write what receive then try to process & send
- Checkpoint: Consumer module remember where we leave, will not miss data if syncer shutdown in accident
- Retry: If output channel fail to send to output target, retry until success or write to failure log
- Failure Log: If retry exceed configured num, write item to failure log for human recheck
- Event Scheduler: to solve *Order Problem* between events which has unchanged primary key
  - `mod`: `mod` integral primary key to make same row change always handled in order;
  - `hash`（default value）: hash primary key of data row, then `mod` hash value to schedule;
  - `direct`: 
    - *No order promise*
    - If your data source has only insert operation, you can choose this scheduler, which is faster;
    - for data source with insert/update/delete, higher output rate if you can endure some inconsistency;

---

If you are changing the `id` of event **but not call `SyncData.setPartitionField(x)`**, it always means you are doing joining like I do, which 
 - may fail consistency promise because the order between events may not scheduled as it should be;
 - may cause dup item because Syncer only make sure `exactly once semantic`;

### Updated Asynchronously
The business database query request is delayed as little as possible 
because Syncer listening change by Master-slave sync (binlog/oplog). 
 

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
### Run
```
git clone https://github.com/zzt93/syncer
cd syncer/ && mvn package
# /path/to/config/: producer.yml, consumer.yml, password-file
# use `-XX:+UseParallelOldGC` if you have less memory and lower input pressure
# use `-XX:+UseG1GC` if you have at least 4g memory and event input rate larger than 2*10^4/s
java -server -XX:+UseG1GC -jar ./syncer-core/target/syncer-core-1.0-SNAPSHOT.jar [--debug] [--port=40000] [--config=/absolute/path/to/syncerConfig.yml] --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
```
Full and usable samples can be found under [`test/config/`](test/config/), like [`test/config/simplest`](test/config/simplest)

## How to ?

If you have any problems with how to use `Syncer` or bugs of it, write a issue.
I will handle it as soon as I can.

## FAQ

- Q: "Got error produce response in correlation id xxx on topic-partition xxx.xxPartition-0, splitting and retrying (5 attempts left). Error: MESSAGE_TOO_LARGE"?
  - A: Adjust message `batch.size` to smaller number or config `kafka` to receive large message


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
- For data warehouse sync

## TODO
[See Issue 1](https://github.com/zzt93/syncer/issues/1)

---

## Implementation
Implementation detail can be found in [doc](doc/)
