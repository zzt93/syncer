# Syncer: sync data from mysql to ...

## Features

### Input

- Table name filter, support regex
- Interested column filter

### Output
- Elasticsearch
  - Bulk operation
  - Update/Delete documents by `UpdateByQuery` or `DeleteByQuery`
  - Join/merge documents from different source when push to ES<sup>[1](#myfootnote1)</sup>
    - One to many relationship (parent-child relationship in ES)for document in different index
    - Self referential relationship handle

- Http endpoint

<a name="myfootnote1">[1]</a>: Be careful about this feature, it may affect your performance

## Limitation

- MySQL config
  - binlog_format: row
  - binlog_row_image: full

- Not support composite primary key
- Not support update primary key
- Only support update/delete by query exact value, i.e. no support query analyzed field (`text` query when update)

## Notice

- Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause unpredictable version conflict
- Update/Delete by query will be executed at once, i.e. will not be buffered or use batch
  

## TODO
- Support set parent of ES
- Remember start file/position of binlog
- Two instance connect to same mysql server bug (sid, cid, serverid)

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
java -jar syncer.jar --pipeline=/absolute/path/to/sample.yml
```

## Implementation

