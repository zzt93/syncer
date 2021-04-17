

- `batch`: support output change in batch
  - `size`: flush if reach this size (if `size` <= 0, it will be considered as buffer as large as possible)
  - `delay`: flush if every this time in `MILLISECONDS`
  - `maxRetry`: max retry if met error
- `failureLog`: failure log config
  - `countLimit`: failure
  - `timeLimit`: failure log item in this time range
- `requestMapping`, `rowMapping`: how to convert `SyncData` to suitable format
and send to where
- `elasticsearch`
  - When using this channel, you may prefer to not include `id` like field in interested column config (`fields`),
  because it is always no need to include it in data field for ES and we will auto detect it and set it for you.
  - e.g.
  ```yml
    elasticsearch:
      connection:
        clusterName: test-${ACTIVE_PROFILE}
        clusterNodes: ["${HOST_ADDRESS}:9300"]
      requestMapping: # mapping from input data to es request
        enableExtraQuery: true
        retryOnUpdateConflict: 3
        upsert: false
        index: "entity + #suffix" # default: repo
        type: "#docType" # default: entity
        documentId: "id" # default: id
        fieldsMapping: # default: fields.*.flatten
          "fields": "fields.*.flatten"
      batch:
        size: 100
        delay: 1000
        maxRetry: 5
      refreshInMillis: 1000
      failureLog:
        countLimit: 1000

  ```
- `mysql`
  - Use
  - e.g.:
  ```yml
    mysql:
      connection:
        address: ${HOST_ADDRESS}
        port: 3306
        user: xxx
        password: xxx
      rowMapping:
        schema: " 'test' "
        table: " 'someTable' "
        id: "id"
        rows:
          "fields": "fields.*.flatten"
      batch:
        size: 100
        delay: 100
        maxRetry: 5
      failureLog:
  ```