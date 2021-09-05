
Full and usable samples can be found under [`test/config/`](../../test/config/)

### producer.yml
- `input[]`
 - `type`: MySQL, Mongo
 - <a name="connection"></a>`connection`: `ip`, `address`, `port`, `user`, `password`, `passwordFile`
 - `file`: absolute path to binlog file

Full and usable samples can be found under [`test/config/`](../../test/config/)

### <a name="consumer_config"></a>consumer.yml

Full and usable samples can be found under [`test/config/`](../../test/config/)

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

Full and usable samples can be found under [`test/config/`](../../test/config/)


### Syncer Config

Usually no need to care, because it is used for meta info of Syncer. Samples can be found in [resources](../../syncer-core/src/main/resources)

