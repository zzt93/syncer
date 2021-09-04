

### Output

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
Full and usable samples can be found under [`test/config/`](test/config/)


### More Detail

- `batch`: support output change in batch
  - `size`: flush if reach this size (if `size` <= 0, it will be considered as buffer as large as possible)
  - `delay`: flush if every this time in `MILLISECONDS`
  - `maxRetry`: max retry if met error
- `failureLog`: failure log config
  - `countLimit`: failure
  - `timeLimit`: failure log item in this time range
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
        retryOnUpdateConflict: 3
        upsert: false
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
      batch:
        size: 100
        delay: 100
        maxRetry: 5
      failureLog:
  ```