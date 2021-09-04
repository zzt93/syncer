
# Syncer: MySQL/MongoDB => Elasticsearch/MySQL/Kafka/HBase

## 特性

- 一致性: 保证【数据源】与【数据目的地】的[数据一致](doc/consistency_CN.md)
- 异步同步: 对数据源极小影响
- 同步并修改: 支持编写Java代码来自定义同步过程[customize sync](doc/detail-feature.md)
- 支持多种数据源：[MySQL/MongoDB to Kafka/ES/MySQL/HBase](doc/detail-feature.md)
- ETL全量冷启动 & 增量同步 无缝结合: 不丢失任何数据


## 使用

### 环境

- MySQL 配置
    - binlog_format: 行
    - binlog_row_image：完整
- MongoDB 配置：
  -（可选）更新`bind_ip` 以允许侦听来自配置地址上的应用程序的连接。
    - 从启用复制集开始：
      -`mongod --replSet myapp`
        - 或者使用 docker: `docker run -d --name mongodb -p 27017:27017 -v /root/mongodb-container/db:/data/db mongo:3.2 mongod --replSet chat`
    - shell 中的初始化复制集：`rs.initiate()`
      
### 运行
```
git clone https://github.com/zzt93/syncer
cd syncer/ && mvn package
# /path/to/config/: producer.yml, consumer.yml, password-file
# use `-XX:+UseParallelOldGC` if you have less memory and lower input pressure
# use `-XX:+UseG1GC` if you have at least 4g memory and event input rate larger than 2*10^4/s
java -server -XX:+UseG1GC -jar ./syncer-core/target/syncer-core-1.0-SNAPSHOT.jar [--debug] [--port=40000] [--config=/absolute/path/to/syncerConfig.yml] --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
```
完整可用的配置文件示例可以在 [`test/config/`](test/config/) 下找到，例如 [`test/config/simplest`](test/config/simplest)

## 如何 ？

如果您对如何使用 `Syncer` 或它的错误有任何问题，请写一个问题。
我会尽快处理的。
 
### 常问问题

- 问：“在主题分区 xxx.xxPartition-0 上的相关 ID xxx 中出现错误，正在拆分和重试（还剩 5 次尝试）。错误：MESSAGE_TOO_LARGE”？
    - A: 将消息 `batch.size` 调整为较小的数字或配置 `kafka` 以接收大消息


### 用于生产
- 搜索系统：搜索数据同步
- 微服务：认证/推荐/聊天数据同步
    - 同步要求：低延迟，高可用
- Join表：避免在生产环境中加入，通过加入表使用空间来提高速度
    - 同步要求：低延迟，高可用
- Kafka：同步数据到kafka，供其他异构系统使用
- 用于数据恢复：如果错误地删除实体，或者您知道从哪里开始和结束
- 用于更改表同步：
    - [旧版本 MySQL 更改表的速度非常慢](https://stackoverflow.com/questions/12774709/mysql-very-slow-for-alter-table-query)
    - [MySQL 8.0: InnoDB 现在支持 Instant ADD COLUMN](https://mysqlserverteam.com/mysql-8-0-innodb-now-supports-instant-add-column/)
- 用于数据仓库同步

## TODO
[见 Issue 1](https://github.com/zzt93/syncer/issues/1)

---

## 实现
实现细节可以在 [impl.md](doc/impl.md) 中找到
