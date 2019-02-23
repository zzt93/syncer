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
    c1=`curl -s -X GET "localhost:49200/test*/${table}/_count" -H 'Content-Type: application/json'`
    echo "[Sync result] -- test*.$table: $c1"
    if [[ ${c1} -ne "$all" ]];then
        echo "ERROR: $table not right"
    fi

    c2=`curl -s -X GET "localhost:49200/test*/news/_count" -H 'Content-Type: application/json' -d'
    {
        "query" : {
            "term" : { "user" : "kimchy" }
        }
    }
    '`
done

echo "-----"
echo "Done $0"
echo "-----"
