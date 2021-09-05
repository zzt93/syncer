## Implementation

- [Elasticsearch & MySQL Sync Challenge(1)](https://tonyz93.blogspot.com/2017/11/elasticsearch-mysql-sync-challenge-1.html)
- [Elasticsearch & MySQL Sync Challenge(2): Event Driven](https://tonyz93.blogspot.com/2017/12/elasticsearch-mysql-sync-challenge-2.html)
- [Elasticsearch & MySQL Sync Challenge(3): Implementation](https://tonyz93.blogspot.com/2017/12/elasticsearch-mysql-sync-challenge-3.html)
- [Elasticsearch & MySQL Sync Challenge(4): Quality Attributes](https://tonyz93.blogspot.com/2018/01/elasticsearch-mysql-sync-challenge-4.html)
- [Elasticsearch & MySQL Sync Challenge(5): Redesign](https://tonyz93.blogspot.com/2018/02/elasticsearch-mysql-sync-challenge-5.html)

### Input Module

- Load yml file into environment property source
- Bind property source into config model class

#### Mongo config
The readConcern option allows you to control the consistency and isolation properties of the data read from replica sets and replica set shards.
Through the effective use of write concerns and read concerns, you can adjust the level of consistency and availability guarantees as appropriate, such as waiting for stronger consistency guarantees, or loosening consistency requirements to provide higher availability.
MongoDB drivers updated for MongoDB 3.2 or later support specifying read concern.

#### Updated Asynchronously
The business database query request is delayed as little as possible
because Syncer listening change by Master-slave sync (binlog/oplog).

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

---

- Shutdown process
  - Producer starter shutdown
    - Connector shutdown
    - Starter service shutdown
  - Consumer starter shutdown
    - Output stater shutdown
      - Output channel shutdown
      - Batch service shutdown
    - Filter-output service shutdown