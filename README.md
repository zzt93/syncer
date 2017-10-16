# Syncer: sync data from mysql to ...

## Limitation

- MySQL config
  - binlog_format: row
  - binlog_row_image: full

- Not support composite primary key
- Not support update primary key

## Notice

- Don't update/delete use `syncer` and other way (REST api or Java api) at the same time, it may cause unpredictable version conflict
- Update/Delete by query will be executed at once, i.e. will not be buffered or use batch
  

## TODO

## Pipeline Config

### Input
MySQL master binlog
### Filter

- statement
- switcher
- foreach

### Output Choice

 - Elasticsearch
 - Http Endpoint
 
## Run
```
syncer.jar --pipeline=sample.yml
```

## Implementation

