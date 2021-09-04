
# Syncer: MySQL/MongoDB => Elasticsearch/MySQL/Kafka/HBase

## 特性

- 一致性: 保证【数据源】与【数据目的地】的数据一致
- 异步同步: 对数据源极小影响
- 同步并修改: 支持编写Java代码来自定义同步过程[customize sync](doc/detail-feature.md)
- 支持多种数据源：[MySQL/MongoDB to Kafka/ES/MySQL/HBase](doc/detail-feature.md)
- ETL全量冷启动 & 增量同步 无缝结合: 不丢失任何数据

---

#### 数据一致性的两个方面

- 最终一致性: 保证数据最终一定能到达【数据目的地】
- 数据顺序问题: 保证发生在【数据源】的数据变更 与 到达【数据目的地】的数据变更，顺序一致 
  
为什么顺序很重要？如下两个例子以供参考，如果顺序错乱，数据将不一致：
- 顺序不一致的话，name最终是'a'还是'b'?
    ```sql
  update t set name='a' where id = 1;
  update t set name='b' where id = 1;
    ```
- 顺序不一致的话，表格中，id为1的数据还在不在？
    ```sql
  insert into t values (1);
  delete from t where id = 1;
    ```
#### 数据一致性的实现

- 主从复制协议：如果网络有问题，MySQL/Mongo 内置主从同步协议，会重新发送丢失的数据包
- `WAL`：Syncer的消费者模块采用`write ahead log`，首先写入接收到的数据变更到持久化存储，然后再尝试处理和发送
- 检查点：Syncer的消费者模块记住我们同步的位置，如果Syncer意外关闭，将不会错过任何数据
- 重试：如果Syncer无法发送数据到【数据目的地】，则重试直到成功或写入失败日志
- 失败日志：如果重试次数超过配置的次数，则将项目写入失败日志以供人工重新检查，避免卡死同步过程
- 变更事件顺序调度器：解决变更事件之间的*顺序问题*
    - `mod`顺序调度器：对主键取余，使同一行更改始终按顺序处理；
    - `hash`顺序调度器（默认值）：hash数据行的主键，然后对散列值取值；
    - `direct`顺序调度器：
        - *无顺序保证*
        - 如果数据源只有插入操作，可以选择这个调度器，速度更快；
        - 虽然数据源有插入/更新/删除，但是能忍受一些数据不一致，输出率更高；

---

如果您正在更改变更事件（SyncData） ** 的 `id`，但不调用 `SyncData.setPartitionField(x)`**，则始终意味着您正在像我一样加入，即
- 可能无法保证一致性，因为事件之间的顺序可能没有按预期安排；
- 可能会导致重复项，因为 Syncer 只确保“恰好一次语义”；

### 异步更新
尽量少延迟业务数据库查询请求
因为主从同步（binlog/oplog）改变了同步器的监听。


## 使用同步器

＃＃＃ 准备

- MySQL 配置
    - binlog_format: 行
    - binlog_row_image：完整
- MongoDB 配置：
  -（可选）更新`bind_ip` 以允许侦听来自配置地址上的应用程序的连接。
    - 从启用复制集开始：
      -`mongod --replSet myapp`
        - 或者使用 docker: `docker run -d --name mongodb -p 27017:27017 -v /root/mongodb-container/db:/data/db mongo:3.2 mongod --replSet chat`
    - shell 中的初始化复制集：`rs.initiate()`
      ＃＃＃ 跑
      ``
      git 克隆 https://github.com/zzt93/syncer
      cd 同步器/ && mvn 包
# /path/to/config/: producer.yml、consumer.yml、密码文件
# 如果内存较少且输入压力较低，请使用 `-XX:+UseParallelOldGC`
# 使用`-XX:+UseG1GC` 如果你有至少 4g 内存和大于 2*10^4/s 的事件输入速率
java -server -XX:+UseG1GC -jar ./syncer-core/target/syncer-core-1.0-SNAPSHOT.jar [--debug] [--port=40000] [--config=/absolute/path/to /syncerConfig.yml] --producerConfig=/absolute/path/to/producer.yml --consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml
``
完整可用的示例可以在 [`test/config/`](test/config/) 下找到，例如 [`test/config/simplest`](test/config/simplest)

＃＃ 如何 ？

如果您对如何使用 `Syncer` 或它的错误有任何问题，请写一个问题。
我会尽快处理的。

＃＃ 常问问题

- 问：“在主题分区 xxx.xxPartition-0 上的相关 ID xxx 中出现错误，正在拆分和重试（还剩 5 次尝试）。错误：MESSAGE_TOO_LARGE”？
    - A: 将消息 `batch.size` 调整为较小的数字或配置 `kafka` 以接收大消息


### 用于生产
- 搜索系统：搜索数据同步
- 微服务：认证/推荐/聊天数据同步
    - 同步要求：低延迟，高可用
- 加入表：避免在生产环境中加入，通过加入表使用空间来提高速度
    - 同步要求：低延迟，高可用
- Kafka：同步数据到kafka，供其他异构系统使用
- 对于数据恢复：如果错误地删除实体，或者您知道从哪里开始和结束
- 对于更改表同步：
    - [MySQL 更改表的速度非常慢](https://stackoverflow.com/questions/12774709/mysql-very-slow-for-alter-table-query)
    - [MySQL 8.0: InnoDB 现在支持 Instant ADD COLUMN](https://mysqlserverteam.com/mysql-8-0-innodb-now-supports-instant-add-column/)
- 用于数据仓库同步

＃＃ 去做
[见问题1](https://github.com/zzt93/syncer/issues/1)

---

＃＃ 执行
实现细节可以在 [doc](doc/) 中找到
