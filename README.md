
# Syncer: MySQL/MongoDB => Elasticsearch/MySQL/Kafka/HBase 

[中文文档](doc/README_CN.md)

## Features

 - Sync data consistently: make sure [data consistency](doc/consistency.md)
 - Sync data async: little effect on origin server
 - Sync data and manipulate: write Java code to [customize sync](doc/config/consumer-filter.md)
 - Sync data from [MySQL/MongoDB to Kafka/ES/MySQL/HBase](doc/detail-feature.md)
 - ETL and real-time data sync combined: missing no data

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
Full and usable sample config can be found under [`test/config/`](test/config/), like [`test/config/simplest`](test/config/simplest)

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
