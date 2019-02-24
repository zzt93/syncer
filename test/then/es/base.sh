#!/usr/bin/env bash

env=$1

echo "-----"
echo "Testing $0"
echo "-----"

# tables in mysql_test.sql
names="news correctness types"

function extractCount() {
    response=$1
    echo ${response} | egrep -o '\{"count":[0-9]+' | awk -F ':' '{print $NF}'
}

# query mysql count
for table in ${names} ; do
    all=`docker-compose -f ${env}.yml exec mysql_0 mysql -uroot -proot -N -B -e "select count(*) from test_0.${table}" | grep -o "[0-9]*"`
    echo "[Sync input] -- test.$table: $all"
    tmp=`curl -s -X GET "localhost:49200/test*/${table}/_count" -H 'Content-Type: application/json'`
    c1=`extractCount ${tmp}`
    echo "[Sync result] -- test*.$table: $c1"
    if [[ ${c1} -ne "$all" ]];then
        echo -e "$RED ERROR $NC: $table not right"
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

echo "-----"
echo "Done $0"
echo "-----"
