
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

If you are changing the `id` of event **but not call `SyncData.setPartitionField(x)`**, which
- may fail consistency promise because the order between events may not scheduled as it should be;
- may cause dup item because Syncer only make sure `exactly once semantic`;
