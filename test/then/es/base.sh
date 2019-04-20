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

function esAssert() {
    instance=$1
    db=$2
    table=$3

    all=`docker-compose -f ${ENV_CONFIG} exec ${instance} mysql -uroot -proot -N -B -e "select count(*) from ${db}.${table}" | grep -o "[0-9]*"`
    logi "[Sync input] -- ${db}.${table}: $all"
    tmp=`curl -s -X GET "localhost:49200/${db}*/${table}/_count" -H 'Content-Type: application/json'`
    c1=`extractCount ${tmp}`
    logi "[Sync result] -- ${db}*.${table} in ES : $c1"
    if [[ ${c1} -ne "$all" ]];then
        loge "$table not right"
    fi

}


for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
    for table in ${names} ; do
        esAssert mysql_${i} test_${i} ${table}
    done
done

# tables in mysql_simple.sql
esAssert mysql_0 simple simple_type


logi "-----"
logi "Done $0"
logi "-----"
