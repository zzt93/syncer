#!/usr/bin/env bash

source ${UTIL_LIB}


env=$1

logi "-----"
logi "Testing $0"
logi "-----"

function mysqlAssert() {
    instance=$1
    db=$2
    table=$3

    all=`extractMySqlCount ${instance} ${db} ${table}`
    logi "[Sync input] -- ${db}.${table}: $all"
    tmp=`extractMySqlResultCount ${instance} ${db} "${table}"`
    logi "[Sync result] -- ${db}.`mysqlResultName ${table}`: $tmp"
    if [[ ${tmp} -ne "$all" ]];then
        loge "$table not right"
    fi
}

function drdsAssert() {
    table=$1

    all=0
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        instance=mysql_${i}
        db=test_${i}
        c=`extractMySqlCount ${instance} ${db} ${table}`
        logi "[Sync input] -- ${db}.${table}: $c"
        let all=all+c
    done

    # see consumer_drds.yml & drds.yml
    instance=mysql_0
    db=test_0
    tmp=`extractMySqlResultCount ${instance} ${db} "${table}"`
    logi "[Sync result] -- ${db}.`mysqlResultName ${table}`: $tmp"
    if [[ ${tmp} -ne "$all" ]];then
        loge "$table not right"
    fi
}

# tables in mysql_test.sql
names="news correctness types"
for table in ${names} ; do
    drdsAssert ${table}
done

# tables in mysql_simple.sql
mysqlAssert mysql_0 simple simple_type


logi "-----"
logi "Done $0"
logi "-----"