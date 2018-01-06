# Syncer: sync data from mysql to ...

## Features

### Consistency

#### Consistent mode: 
  - `strict`: make sure output is successful, otherwise, retry until success
  - `loose`: try to send to output

#### Consistency Promise

- If network has problem, MySQL master slave protocol?
- Input module remember where we leave, try to not miss data if syncer shutdown in accident
- If data is still not send in output channel when input is starting to handle next event, and syncer shutdown?
- If output channel fail to send to output target, retry until success


### Input

- Remember where we leave last time, restart at that file/position of binlog
  - Now it store meta info in `/tmp/syncer/meta/.last_position`, but it can also be configured using your own config file `--config=config.yml`
  - If we stop syncer too long, the position it stored may be illegal because the clean up of binlog file.
  At that time, syncer will re-connect to latest binlog (may be opted to connect to oldest binlog)
- Schema filter, support regex
- Table name filter
- Interested column filter

#### Notice:

- If an event match multiple schema & table, we will use the first match to filter/output

### Output

- Elasticsearch: strict
  - Bulk operation
  - Update/Delete documents by `UpdateByQuery` or `DeleteByQuery`
  - Join/merge documents from different source when push to ES<sup>[1](#join_in_es)</sup>
    - One to many relationship (parent-child relationship in ES)for document in different index
    - Self referential relationship handle

- Http endpoint: loose
- MySQL connection: strict

<a name="join_in_es">[1]</a>: Be careful about this feature, it may affect your performance

## Limitation

- MySQL config
  - binlog_format: row
  - binlog_row_image: full

- Not support composite primary key
- Not support update primary key
- Only support update/delete by query exact value, i.e. no support query analyzed field (`text` query when update)

## Notice

- Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause version conflict and fail the change
- Update/Delete by query will be executed at once, i.e. will not be buffered or use batch
  

## TODO
- Support set parent of ES
- Row image format support?
  - Add must appeared field restriction -- now only primary key
  - Opt: keep only changed field in update event & primary key in delete event -- include must appear field
- Join by query mysql?
- Make all output channel strict

## Pipeline Config

### Input
MySQL master binlog
### Filter

- statement
- switcher
  - support `default` case
  - only execute one case
- foreach
- if
  - clone
  - drop
  - statement

### Output Choice

 - Elasticsearch
 - Http endpoint
 
## Run
```
java -jar syncer.jar --pipelineConfig=/absolute/path/to/sample.yml
```

## Implementation

### Input Module

- Load yml file into environment property source
- Bind property source into config model class

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
  - Maybe change to jackson