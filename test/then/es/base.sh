#!/usr/bin/env bash

env=$1

all=`docker-compose -f ../../${env}.yml exec mysql_0 mysql -uroot -proot -N -B -e 'select count(*) from test_0.news' | grep -o "[0-9]*"`
echo "[Sync input] -- test.news: $all"


c1=`curl -s -X GET "localhost:49200/test*/news/_count" -H 'Content-Type: application/json'`
echo "[Sync result] -- test*: $c1"
if [[ ${c1} -eq "$all" ]];then
    exit 1
fi

c2=`curl -s -X GET "localhost:49200/test*/news/_count" -H 'Content-Type: application/json' -d'
{
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
'`