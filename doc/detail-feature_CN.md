### Input —— 数据源

- 支持同时监听阿里云的MySQL & MongoDB & DRDS (https://www.aliyun.com/product/drds)
- 如果 Syncer 无法连接到输入数据源，将中止启动
- MySQL 监听功能：
  - 数据库（名为`repos`）过滤器，支持正则表达式
    - 如果一个表匹配多个数据库和表（因为使用了正则表达式），Syncer将记录错误消息到日志，
      并随机使用一个匹配的过滤器列
  - 表名过滤器
  - 列过滤器
    - 如果变更事件通过列过滤器，并且只剩下主键：
      - 如果变更事件类型为`UPDATE`，则丢弃该变更事件——因为不支持更新id；
      - 其他变更事件类型，保留它。
  - 在 `UPDATE` 中，所有用户感兴趣的列都可以取到，即使部分列没有变化（与 `MongoDB` 不同）
  - 自动检测表的主键并设置进入`SyncData#id`
  - 支持读取binlog文件进行数据丢失恢复(`input[x].file`)
  - 支持指定binlog文件/位置开始读取（`input[x].connection.syncMeta[]`）
- MongoDB 主源过滤器：
  - 版本：3.x、4.0
    - 只有 4.0 支持字段移除检测和同步（由于 ES/MySQL 的限制，它总是意味着在输出目标中将字段设置为空，这可能不是你想要的）
  - 数据库（名为`repos`）过滤器，支持正则表达式
    - 如果一个变更事件匹配多个模式和表，我们将使用第一个明确的配置(配置文件中的顺序)来过滤/输出，
      即明确的`repo` 配置将覆盖正则表达式`repo` 配置
  - 集合名称过滤器
  - 列过滤器
    - 如果变更事件通过列过滤器，并且只剩下主键：
      - 如果变更事件类型为`UPDATE`，则丢弃该变更事件——因为不支持更新id；
      - 其他变更事件类型，保留它。
  - 在 `UPDATE` 中，只会收到更改的列（与 `MySQL` 不同）
  - 自动`_id`检测并设置为`SyncData#id`
  - 如果为Mongo配置了用户名&密码，此用户应该有`[listDatabases, find]`的权限
  - 只支持监听一级字段（因为MongoDB存储json，可能有多个级别）
- DRDS：
  - 配置与 MySQL 相同，但需要直接连接到 RDS 的 MySQL，因为 DRDS 不支持 binlog dump
  - 记得在 `fields` 中添加分区键

---
#### 其他非功能特性

- 通过写入binlog/oplog的文件/位置来记住我们上次离开的地方，并从那里恢复以避免任何数据丢失
  - 不止一次（至少一次）：我们现在可以确保至少一次语义，所以你需要确保你的输出通道（同步器输出的`消费者`）
    是**幂等的**，您的目的地可以处理它而不会重复。反例：没有主键的表肯定
    无法处理它并迟早会导致重复数据。
- 多个消费者可以共享到同一数据源的公共连接，即 MySQL/MongoDB，以减少
  远程主数据源的负担
- 根据注册信息自动跳过消费者的同步项目

---

数据项从`Input`模块出来后，转换为`SyncData`(s)—— 数据变更的抽象。
换句话说，单个 binlog 项可能包含多行更改，所以会转换到多个“SyncData”。

### Filter——操作

通过以下方式操作`SyncData`：

- `sourcePath`: 编写一个java类来处理`SyncData`（更多细节见*[Consumer Pipeline Config](config/consumer-filter.md)*的过滤器部分）
- 或者如果不需要操作则直接跳过这部分配置


### Output——数据接收器

- 如果输出通道遇到太多失败/错误（超过`countLimit`），它将中止并将健康更改为`red`
- 如果无法连接到输出通道，将每 2**n 秒重试一次
- Elasticsearch
  - 版本：5.x
  - 批量操作
  - 通过`UpdateByQuery` 或`DeleteByQuery` 更新/删除文档
  - 推送到 ES 时Join/合并来自不同来源的文档<sup>[1](#join_in_es)</sup>
    - ExtraQuery：进行额外查询以获取额外需要的信息
    - 不同索引中文档的一对多关系（ES中的父子关系）
    - 自引用关系处理
  - 添加`upsert`支持，修复`DocumentMissingException`使用`upsert`，可用于以下两种场景
    - 初始化数据加载，通过手动创建索引并将同步字段更新到ES（仅支持`MySQL`输入）
    - 修复一些意外的配置/同步错误
  - 无需其他代码进行搜索数据准备

- MySQL
  - [版本](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-versions.html): 5.5, 5.6, 5.7, 8.0
  - 批量操作
  - 简单的嵌套 sql：`insert into select`
  - 忽略`DuplicateKeyException`，不计为失败
  - **低延迟**
- Kafka
  - [版本](https://www.confluent.io/blog/upgrading-apache-kafka-clients-just-got-easier/): 0.10.0 或更高版本
  - 批量操作
  - 使用数据源的`id`作为记录的`key`，确保[记录之间的顺序](https://stackoverflow.com/questions/29511521/is-key-required-as-part-of-sending-消息到 kafka）
  - 使用 `SyncResult` 作为 msg `data`
  - Json 序列化器/反序列化器
  - **注意**：Kafka msg的消费者必须处理变更事件幂等；
  - **注意**：如果发生错误，可能会[混乱](https://stackoverflow.com/questions/46127716/kafka-ordering-guarantees)；
  - 易于重新使用，重建而不影响商业数据库；
- HBase
  - [版本](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Compatibility.html#Wire_Protocols)

<a name="join_in_es">[1]</a>：小心这个功能，它可能会影响你的同步速率

### 其他
- Http 端口
  - 端口决定：
    - 如果没有端口配置，`Syncer` 将尝试在 `[40000, 40010)` 之间的端口
    - 如果端口是通过命令行或 `config.yml` 中的 env var `port` 或 `port` 配置的
      同步器将使用该端口
    - 如果端口在多个位置配置：命令行、环境变量和配置文件，优先级为
      - 命令行选项
      - 环境变量
      - 文件配置
  - `http://ip:port/health`：动态报告`Syncer` 状态；

- JMX 
  - 使用`jconsole`连接`Syncer`，可以动态【更改日志级别】(https://logback.qos.ch/manual/jmxConfig.html)； （或者在启动时通过`--debug`选项更改日志级别）


### 限制
- MySQL：
  - 不要更改binlog的数字后缀命名，否则会导致binlog选择失败
  - 支持的版本：依赖于这个 [binlog connector lib](https://github.com/shyiko/mysql-binlog-connector-java)
  - 不支持复合主键
  - 不支持更新主键
  - 如果有额外查询，仅支持按查询精确值更新/删除，即不支持查询分析字段（更新时为`text`查询）
  - 无论列定义是否包含“无符号”关键字，数字类型（tinyint 等）的数据始终返回 **signed**。
    如有必要，您可能需要转换为无符号。
    - 如果您的同步目的地是 MySQL，Syncer 会直接为您处理这种情况，您无需处理
      ``
      Byte.toUnsignedInt((byte)(int) fields['xx'])
      // 或者
      SyncUtil.unsignedByte(sync, "xx");
      ``
  - `*text`/`*blob` 类型的数据总是以字节数组的形式返回（在将来的版本，`var*`也会变成数组）。
    如有必要，您可能需要转换为字符串。
    - 如果您的输出是 MySQL，Syncer 会为您处理这种情况。
      ``
      new String(fields['xx'])
      // 或者
      SyncUtil.toStr(sync, "xx");
      ``
- Mongo：
  - 如果同步到 ES，则不会从 ES 中删除字段
  - [兼容性](https://docs.mongodb.com/ecosystem/drivers/java/#mongodb-compatibility)
  - 对于 4.0 及更高版本（使用 [change stream](https://docs.mongodb.com/manual/changeStreams/)）：
    - 存储引擎：WiredTiger
    - 副本集协议版本：副本集和分片集群必须使用副本集协议版本 1 (pv1)。
    - [Read Concern “majority”](https://docs.mongodb.com/manual/reference/read-concern-majority/#readconcern.%22majority%22) 已启用。

- ES
  - 不要同时更新/删除使用`syncer`和其他方式（REST api或Java api），这可能会导致版本冲突并导致更改失败
  - Update/Delete-by-query 将立即执行，即不会被缓冲或使用批处理