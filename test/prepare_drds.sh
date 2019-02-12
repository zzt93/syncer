#!/usr/bin/env bash

docker-compose -f drds.yml up

tmp=`mktmp`
for (( i = 0; i < 3; ++i )); do
    echo "create database test_$i; use test_$i;" > $tmp
    cat mysql_init.sql >> $tmp
    cat $tmp | mysql -uroot -h localhost -proot
done

