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
  - 集合名称过滤器
  - 列过滤器
    - 如果变更事件通过列过滤器，并且只剩下主键：
      - 如果变更事件类型为`UPDATE`，则丢弃该变更事件——因为不支持更新id；
      - 其他变更事件类型，保留它。
  - 在 `UPDATE` 中，只会收到更改的列（与 `MySQL` 不同）
  - 自动`_id`检测并设置为`SyncData#id`
  - 如果一个变更事件匹配多个模式和表，我们将使用第一个明确的配置(配置文件中的顺序)来过滤/输出，
    即明确的`repo` 配置将覆盖正则表达式`repo` 配置
  - 如果为auth配置用户/密码，它应该有`[listDatabases, find]`的权限
  - 只支持监听一级字段（因为MongoDB存储json，可能有多个级别）
- DRDS：
  - 配置与 MySQL 相同，但需要直接连接到 RDS 的 MySQL，因为 DRDS 不支持 binlog dump
  - 记得在 `fields` 中添加分区键

---

- 通过写入binlog/oplog的文件/位置来记住我们上次离开的地方，并从那里恢复以避免任何数据丢失
  - 不止一次（至少一次）：我们现在可以确保至少一次语义，所以你需要确保你的输出通道（同步器输出的`消费者`）
    是**幂等的**，您的目的地可以处理它而不会重复。反例：肯定没有主键的表
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
- 弹性搜索
  - 版本：5.x
  - 批量操作
  - 通过`UpdateByQuery` 或`DeleteByQuery` 更新/删除文档
  - 推送到 ES 时加入/合并来自不同来源的文档<sup>[1](#join_in_es)</sup>
    - ExtraQuery：进行额外查询以获取额外需要的信息
      - 通过特殊标记`$var$` 支持多个额外的依赖查询
    - 不同索引中文档的一对多关系（ES中的父子关系）
    - 自引用关系句柄
  - 添加`upsert`支持，修复`DocumentMissingException`使用`upsert`，可用于以下两种场景
    - 初始化数据加载，通过手动创建索引并将同步字段更新到ES（仅支持`MySQL`输入）
    - 修复一些意外的配置/同步错误
  - 除配置外，无需代码进行搜索数据准备

- MySQL
  - [版本](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-versions.html): 5.5, 5.6, 5.7, 8.0
  - 批量操作