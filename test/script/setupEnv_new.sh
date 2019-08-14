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

    cp ${CONFIG_DIR}/${syncerDir}/producer.yml data/config/producer.yml
    cp ${CONFIG_DIR}/${syncerDir}/consumer/* data/config/consumer/
}


function prepareEnv() {
    cp ${CONFIG_DIR}/my.cnf ${TEST_DATA_DIR}/

    logi "----------------"
    logi " docker-compose "
    logi "----------------"
    docker-compose -f ${ENV_CONFIG} up -d

    if [[ ${env} = "mongo" ]]; then
        dockerExec mongo mongo --eval "rs.initiate()"
    fi
}


setupDefaultPara
setupSyncerConfig

logi "Using env=$env, syncerDir=${syncerDir}"


prepareEnv