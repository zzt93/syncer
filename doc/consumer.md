
Full and usable samples can be found under [`test/config/`](test/config/)

### producer.yml
- `input[]`
 - `type`: MySQL, Mongo
 - <a name="connection"></a>`connection`: `ip`, `address`, `port`, `user`, `password`, `passwordFile`
 - `file`: absolute path to binlog file

```yaml

input:
- connection:
    address: ${HOST_ADDRESS}
    port: 3306
    user: xxx
    password: yyy

- connection:
    address: ${HOST_ADDRESS}
    port: 27018
  type: mongo
```
### <a name="consumer_config"></a>consumer.yml

```yaml
version: 1.3

consumerId: simplest


input:
  - connection:
      clusterNodes: [${MYSQL_ADDR}]
    repos:
      - name: "test_0"
        entities:
          - name: correctness
          - name: types
          - name: news
      - name: "simple_0"
        entities:
          - name: simple_type

filter:
  sourcePath: /absolute/path/to/FilterXX.java

output:
  mysql:
    connection:
      address: ${MYSQL_OUT}
      port: 3306
      user: root
      password: ${MYSQL_OUT_PASS}
  elasticsearch:
    connection:
      clusterName: ${ES_CLUSTER}
      clusterNodes: ["${ES_ADDR}:9300"]
```

#### Input
- `input[]`:
  - `type`: same as producer
  - `connection`: [same as producer](#connection)
  - `syncMeta`:
    - `binlogFilename`: string name of remote master's binlog file name
    - `binlogPosition`: position you want to start listening
  - `repos[x]`:
    - `name`: repo name, allow regex
    - `entities[x]`:
      - `name`: entity name
      - `fields`: entity fields list, can omit it which represents all fields
  - `scheduler`:
    - `mod`: `mod` integral primary key to make same row change always handled in order;
    - `hash`: hash primary key of data row, then `mod` hash value to schedule -- default value now;
    - `direct`: 
      - If your data source has only insert operation, you can choose this scheduler, which is faster;
      - *No order promise* for data source with insert/update/delete, higher output rate if you can endure some inconsistency;
  - `onlyUpdated`: whether sync not `updated` event (only for `MySQL`)
    - `updated` definition: `Objects.deepEquals` == true 

```yml
input:
  - connection:
      clusterNodes: [${MYSQL_ADDR}]
    repos:
      - name: "test_0"
        entities:
          - name: correctness
          - name: types
          - name: news
      - name: "simple_0"
        entities:
          - name: simple_type
```
#### Filter


- `sourcePath` (**preferred: more powerful and easier to write**) : write a java class implements `MethodFilter`  to handle `SyncData`
  - Open a new maven project and import latest dependency (not import other dependency):
  ```xml
        <dependency>
            <groupId>com.github.zzt93</groupId>
            <artifactId>syncer-data</artifactId>
            <version>1.0.2-SNAPSHOT</version>
        </dependency>

  ```
  - Write a class implement `MethodFilter` **without package name**: use all api provided by `SyncData` to do any change you like
  ```java
    import com.github.zzt93.syncer.data.SyncData;
    import com.github.zzt93.syncer.data.util.MethodFilter;
    import com.github.zzt93.syncer.data.util.SyncUtil;
    
    import java.util.List;
    
    /**
     * @author zzt
     */
    public class Simplest implements MethodFilter {
      @Override
      public void filter(List<SyncData> list) {
        SyncData sync = list.get(0);
        SyncUtil.unsignedByte(sync, "tinyint");
        SyncUtil.unsignedByte(sync, "type");
      }
    }

  ```
  - config it:
  ```yaml
  filter:
    sourcePath: /data/config/consumer/ColdFilter.java
  ```
  - debug code
    - add log if in production env by slf4j logger
    - [how to add breakpoint to debug?](https://github.com/zzt93/syncer/issues/18)



#### Output

- `elasticsearch`
  - When using this channel, you may prefer to not include `id` like field in interested column config (`fields`),
  because it is always no need to include it in data field for ES and we will auto detect it and set it for you.
  - e.g.
  ```yml
  elasticsearch:
    connection:
      clusterName: ${ES_CLUSTER}
      clusterNodes: ["${ES_ADDR}:9300"]

  ```
- `mysql`
  - e.g.:
  ```yml
  mysql:
    connection:
      address: ${MYSQL_OUT}
      port: 3306
      user: root
      password: ${MYSQL_OUT_PASS}
  ```
For more config, see [doc/output.md](doc/output.md)

### Syncer Config

Usually no need to care, because it is used for meta info of Syncer. Samples can be found in [resources](syncer-core/src/main/resources)

