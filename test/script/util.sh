#!/usr/bin/env bash

source ${LOG_LIB}

function dockerExec() {
  local service="$1"; shift
  docker exec -i $(docker-compose -f ${ENV_CONFIG} ps -q ${service}) "$@"
}

function extractESCount() {
    local instance=$1
    local db=$2
    local table=$3

    local response=`curl -s -X GET "localhost:49200/${db}*/${table}/_count" -H 'Content-Type: application/json'`
    logi ${response} | egrep -o '\{"count":[0-9]+' | awk -F ':' '{print $NF}'
}


function extractMySqlCount() {
    local instance=$1
    local db=$2
    local table=$3

    dockerExec ${instance} mysql -uroot -proot -N -B -e "select count(*) from ${db}.${table}" | grep -o "[0-9]*"
}


export mysqlResultSuffix="_bak"

function extractMySqlResultCount() {
    local instance=$1
    local db=$2
    local table="$3$mysqlResultSuffix"

    extractMySqlCount ${instance} ${db} ${table}
}

function extractMongoCount() {
    local instance=$1
    local db=$2
    local table=$3

    dockerExec mongo mongo ${db} --quiet --eval "db.${table}.count()"
}

function generateInitSqlFile() {
    logi "-------------------"
    logi "generateInitSqlFile"
    logi "-------------------"

    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        tmp="data/mysql_init_${i}.sql"
        if [[ ! -e ${tmp} ]];then
            echo -e "CREATE DATABASE IF NOT EXISTS test_$i;\n use test_$i;" > ${tmp}
            cat generator/mysql_test.sql >> ${tmp}
            cat generator/mysql_simple.sql >> ${tmp}
        fi
        export mysql_init_${i}=$(pwd)/${tmp}
    done
}


function configEnvVar() {
    env=$1
    if [[ ${env} = "mysql" ]]; then
        export MYSQL_INSTANCE=1
    elif [[ ${env} = "drds" ]]; then
        export MYSQL_INSTANCE=3
    elif [[ ${env} = "mongo" ]]; then
        export MYSQL_INSTANCE=0
    else
        loge "Unsupported env"
        exit 1
    fi
    export ENV_CONFIG=`pwd`/docker-compose/${env}.yml

    if [[ ${env} != "mongo" ]]; then
        generateInitSqlFile
    fi
}

function waitSyncer() {
    times=$1
    if [[ -z ${times} ]]; then
        times=3
    fi
    for (( i = 0; i < $times; ++i )); do
        printf "Waiting Syncer to warm up .\r"
        sleep 1
        printf "Waiting Syncer to warm up ..\r"
        sleep 1
        printf "Waiting Syncer to warm up ...\r"
        sleep 1
        echo ""
    done
}

function extractConst() {
    echo $4
}

# tables in mysql_test.sql
names="news correctness types"

function cmpFromTo() {
    waitSyncer

    fromF=$1
    toF=$2
    expected=$3
    hasError=false
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        for table in ${names} ; do
            instance=mysql_${i}
            db=test_${i}

            from=`${fromF} ${instance} ${db} ${table} ${expected}`
            logi "[Sync input] -- ${db}.${table}: $from"
            to=`${toF} ${instance} ${db} ${table} ${expected}`
            logi "[Sync result] -- ${db}*.${table} in ES : $to"
            if [[ ${to} -ne "$from" ]];then
                loge "$table not right"
                hasError=true
            fi
        done
    done

    # tables in mysql_simple.sql
    instance=mysql_0
    db=simple
    table=simple_type

    from=`${fromF} ${instance} ${db} ${table} ${expected}`
    logi "[Sync input] -- ${db}.${table}: $from"
    to=`${toF} ${instance} ${db} ${table} ${expected}`
    logi "[Sync result] -- ${db}*.${table} in ES : $to"
    if [[ ${to} -ne "$from" ]];then
        loge "$table not right"
        hasError=true
    fi


    if [[ ${hasError} = true ]]; then
        exit 77
    fi
}


function cleanupAll() {
    docker-compose -f docker-compose/${env}.yml rm -fsv
    rm -rf data/*
}