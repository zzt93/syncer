#!/usr/bin/env bash

source ${LOG_LIB}


env=$1

logi "-----"
logi "Testing $0"
logi "-----"

function mysqlAssert() {
    instance=$1
    db=$2
    table=$3

    all=`docker-compose -f ${ENV_CONFIG} exec ${instance} mysql -uroot -proot -N -B -e "select count(*) from ${db}.${table}" | grep -o "[0-9]*"`
    logi "[Sync input] -- ${db}.${table}: $all"
    tmp=`docker-compose -f ${ENV_CONFIG} exec ${instance} mysql -uroot -proot -N -B -e "select count(*) from ${db}.${table}_bak" | grep -o "[0-9]*"`
    logi "[Sync result] -- ${db}.${table}_bak: $tmp"
    if [[ ${tmp} -ne "$all" ]];then
        loge "$table not right"
    fi
}

# tables in mysql_test.sql
names="news correctness types"
for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
    for table in ${names} ; do
        mysqlAssert mysql_${i} test_${i} ${table}
    done
done

# tables in mysql_simple.sql
mysqlAssert mysql_0 simple simple_type


logi "-----"
logi "Done $0"
logi "-----"