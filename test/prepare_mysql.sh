#!/usr/bin/env bash


docker build . -f DataGenerator.Dockerfile -t generator:test
docker run -v config/:/data/ generator:test mysql_init.sql 10000
docker run -v config/:/data/ generator:test mysql_simple.sql 10000


docker-compose -f mysql.yml up -d

tmp=`mktmp`
echo "create database test; use test;" > $tmp
cat mysql_init.sql >> $tmp
cat $tmp | mysql -uroot -h localhost -proot -P43306

docker run -v config/:/data/ --rm mysql:5.7.15 mysqlimport --fields-terminated-by=, --verbose -u root -proot -P43306 test /data/*.csv


