#!/usr/bin/env bash

source ${LOG_LIB}

function dockerExec() {
  local service="$1"; shift
  docker exec -i $(docker-compose -f ${ENV_CONFIG} ps -q ${service}) $@
}

function extractESCount() {
    local db=$1
    local table=$2

    local response=`curl -s -X GET "localhost:49200/${db}*/${table}/_count" -H 'Content-Type: application/json'`
    logi ${response} | egrep -o '\{"count":[0-9]+' | awk -F ':' '{print $NF}'
}


function extractMySqlCount() {
    local instance=$1
    local db=$2
    local table=$3

    docker exec -i $(docker-compose -f ${ENV_CONFIG} ps -q ${instance}) mysql -uroot -proot -N -B -e "select count(*) from ${db}.${table}" | grep -o "[0-9]*"
}


export mysqlResultSuffix="_bak"

function extractMySqlResultCount() {
    local instance=$1
    local db=$2
    local table="$3$mysqlResultSuffix"

    extractMySqlCount ${instance} ${db} ${table}
}

function extractMongoCount() {
    local db=$1
    local table=$2

    dockerExec mongo mongo ${db} --quiet --eval "db.${table}.count()"
}