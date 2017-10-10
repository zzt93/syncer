# Syncer: sync data from mysql to ...

## Limitation

- MySQL config
  - binlog_format: row
  - binlog_row_image: full

- Not support composite primary key
- Not support update primary key
  
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

