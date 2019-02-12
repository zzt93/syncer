#!/usr/bin/env bash

tmp=`mktmp`

docker-compose -f mysql.yml up

echo 'create database test; use test;' > $tmp
cat mysql_init.sql >> $tmp
cat $tmp | mysql -uroot -h localhost -proot
# load data



