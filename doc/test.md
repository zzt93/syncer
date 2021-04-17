
## Test
### Dependency
- [Docker](https://docs.docker.com/install/#server)
  - [vm.max_map_count](https://stackoverflow.com/questions/41192680/update-max-map-count-for-elasticsearch-docker-container-mac-host) may be need to
  be set for some os for ES docker image to run
- [Docker compose](https://docs.docker.com/compose/install/)

### Integration Test
#### Test data: 
  - size: 7M
  - machines: 3
  - databases: 3 in logic, after horizontal split is 24
  - tables: 90+ for each database; listening: 5 for each database
  - types: bigint, varchar, text, tinyint, timestamp, smallint, int, unsigned, longtext
#### How
- Insert/load data, count in mysql & es and compare numbers;
- Delete data, count in mysql & es and compare numbers;

### Pressure Test
- 10G & 10^8 lines
  - load every 10^5 lines by `mysqlimport`
  - no pause between import
- Throughput
  - MySQL output: 1300+ insert/s
  ```bash
    time: 20190407-022652
    src=800000
    dst=9302
    time: 20190407-022654
    src=800000
    dst=12070
    time: 20190407-022656
    src=800000
    dst=14863
    time: 20190407-022658
    src=800000
    dst=17536
  ```
  - ES output: 10000+ insert/s
  ```bash
    time: 20190406-083524
    src=800000
    dst=79441
    time: 20190406-083527
    src=800000
    dst=130193
    time: 20190406-083530
    src=800000
    dst=134752
    time: 20190406-083533
    src=800000
    dst=190517
  ```
- CPU: 80-90
- Memory: 4g
  - Increase batch size & flush period, increase performance in cost of higher memory usage (only for ES)
- IO
  - Network
  - Disk
- JVM
  - Thread
  - Lock contention 
