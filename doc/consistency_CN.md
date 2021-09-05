
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
        - 虽然数据源有插入/更新/删除，但是能忍受一些数据不一致，同步速率更高；

---

如果您正在更改 变更事件（SyncData）的 `id`，但没有调用 `SyncData.setPartitionField(x)`，则
- 可能无法保证一致性，因为事件之间的顺序可能没有按预期安排；
- 可能会导致重复项，因为 Syncer 只确保“至少一次语义”；