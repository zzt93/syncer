#!/usr/bin/env bash

env=$1

echo "-----"
echo "Testing $0"
echo "-----"

# tables in mysql_test.sql
names="news correctness types"

# query mysql count
for table in ${names} ; do
    all=`docker-compose -f ${env}.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}" | grep -o "[0-9]*"`
    echo "[Sync input] -- test.$table: $all"
    tmp=`docker-compose -f $env.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}_bak" | grep -o "[0-9]*"`
    echo "[Sync result] -- test.${table}_bak: $tmp"
    if [[ ${tmp} -ne "$all" ]];then
        echo "ERROR: $table not right"
    fi
done


echo "-----"
echo "Done $0"
echo "-----"