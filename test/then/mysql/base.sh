#!/usr/bin/env bash

source ${LOG_LIB}


env=$1

logi "-----"
logi "Testing $0"
logi "-----"

# tables in mysql_test.sql
names="news correctness types"

# query mysql count
for table in ${names} ; do
    all=`docker-compose -f ${env}.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}" | grep -o "[0-9]*"`
    logi "[Sync input] -- test.$table: $all"
    tmp=`docker-compose -f $env.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}_bak" | grep -o "[0-9]*"`
    logi "[Sync result] -- test.${table}_bak: $tmp"
    if [[ ${tmp} -ne "$all" ]];then
        loge "$table not right"
    fi
done


logi "-----"
logi "Done $0"
logi "-----"