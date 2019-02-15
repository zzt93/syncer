#!/usr/bin/env bash


bash generator.sh 10000


docker-compose -f mysql.yml up -d

tmp=`mktmp`
echo "create database test; use test;" > ${tmp}
cat mysql_init.sql >> ${tmp}
cat ${tmp} | mysql -uroot -h localhost -proot -P43306
cat mysql_simple.sql | mysql -uroot -h localhost -proot -P43306

docker run -v data/:/data/ --rm mysql:5.7.15 mysqlimport --fields-terminated-by=, --verbose -u root -proot -P43306 test /data/*.csv


