#!/usr/bin/env bash


docker build . -f DataGenerator.Dockerfile -t generator:test
docker run generator:test mysql_init.sql 10000
docker run generator:test mysql_simple.sql 10000

docker-compose -f drds.yml up -d

tmp=`mktmp`
for (( i = 0; i < 3; ++i )); do
    echo "create database test_$i; use test_$i;" > $tmp
    cat mysql_init.sql >> $tmp
    port=$((43306+$i))
    cat $tmp | mysql -uroot -h localhost -proot -P$port
    docker run -v data/:/data/ --rm mysql:5.7.15 mysqlimport --fields-terminated-by=, --verbose --local -u root -proot -P$port test_$i /data/*.csv
done

