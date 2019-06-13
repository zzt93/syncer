#!/usr/bin/env bash


source ${UTIL_LIB}


logi "-----"
logi "Testing $0"
logi "-----"


function esAssert() {
    db=$1
    table=$2

    all=`extractMongoCount ${db} ${table}`
    logi "[Sync input] -- ${db}.${table}: $all"
    c1=`extractESCount ${db} ${table}`
    logi "[Sync result] -- ${db}*.${table} in ES : $c1"
    if [[ ${c1} -ne "$all" ]];then
        loge "$table not right"
    fi

}

names="test"
for table in ${names} ; do
    esAssert mongo-test ${table}
done



logi "-----"
logi "Done $0"
logi "-----"
