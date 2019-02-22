#!/usr/bin/env bash

env=$1


names="news correctness types"

# query mysql count
for table in $names ; do
    all=`docker-compose -f ${env}.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}" | grep -o "[0-9]*"`
    echo "[Sync input] -- test.news: $all"
    tmp=`docker-compose -f $env.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}_bak" | grep -o "[0-9]*"`
    echo "[Sync result] -- test.news_bak: $tmp"
    if [[ ${tmp} -eq "$all" ]];then
        exit 1
    fi
done


