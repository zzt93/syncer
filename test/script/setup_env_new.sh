#!/usr/bin/env bash
set -e

source ${UTIL_LIB}

env=$1
syncerDir=$2


function setupDefaultPara() {
    if [[ -z "${env}" ]]; then
        env="mysql"
    fi
    if [[ -z "${syncerDir}" ]]; then
        loge "no syncer config specified"
        exit 1
    fi
}

function setupSyncerConfig() {
    logi "Prepare syncer config"

    mkdir -p ${TEST_DATA_DIR}/config/consumer
    rm -f ${TEST_DATA_DIR}/config/consumer/*

    cp ${CONFIG_DIR}/${syncerDir}/producer.yml ${TEST_DATA_DIR}/config/producer.yml
    cp ${CONFIG_DIR}/${syncerDir}/consumer/* ${TEST_DATA_DIR}/config/consumer/
    cp ${TEST_DIR}/../config-sample/src/main/java/* ${TEST_DATA_DIR}/config/consumer/
}


function prepareEnv() {
    cp ${CONFIG_DIR}/my.cnf ${TEST_DATA_DIR}/

    logi "----------------"
    logi " docker-compose "
    logi "----------------"
    docker-compose -f ${ENV_CONFIG} up -d

    if [[ ${env} == mongo* ]]; then
        dockerExec mongo mongo --eval "rs.initiate(); db.getSiblingDB('simple_0').createCollection('simple_type')"
    fi

    # prepare es template
    cp ${CONFIG_DIR}/template.json ${TEST_DATA_DIR}/
    dockerExec elasticsearch curl -XPUT "http://localhost:9200/_template/time_template" -H 'Content-Type: application/json' -d@/Data/template.json
}


setupDefaultPara
setupSyncerConfig

logi "Using env=$env, syncerDir=${syncerDir}"


prepareEnv