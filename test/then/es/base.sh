#!/usr/bin/env bash

env=$1

source ${LOG_LIB}


logi "-----"
logi "Testing $0"
logi "-----"

# tables in mysql_test.sql
names="news correctness types"

function extractCount() {
    response=$1
    logi ${response} | egrep -o '\{"count":[0-9]+' | awk -F ':' '{print $NF}'
}

# query mysql count
for table in ${names} ; do
    all=`docker-compose -f ${env}.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}" | grep -o "[0-9]*"`
    logi "[Sync input] -- test.$table: $all"
    tmp=`curl -s -X GET "localhost:49200/test*/${table}/_count" -H 'Content-Type: application/json'`
    c1=`extractCount ${tmp}`
    logi "[Sync result] -- test*.$table: $c1"
    if [[ ${c1} -ne "$all" ]];then
        loge "$table not right"
    fi

    tmp=`curl -s -X GET "localhost:49200/test*/news/_count" -H 'Content-Type: application/json' -d'
    {
        "query" : {
            "term" : { "user" : "kimchy" }
        }
    }
    '`
    c2=`extractCount ${tmp}`

done

logi "-----"
logi "Done $0"
logi "-----"
