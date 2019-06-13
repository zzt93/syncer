#!/usr/bin/env bash
set -e

env=$1
build=$2
config=$3
num=$4


function configEnvVar() {
    cd test

    export LOG_LIB=`pwd`/script/log.sh
    source ${LOG_LIB}
    export UTIL_LIB=`pwd`/script/util.sh

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
    export TEST_DIR=`pwd`
    export TEST_CONFIG_DIR=`pwd`/config
    export TEST_DATA_DIR=`pwd`/data
}

function setupDefaultPara() {
    if [[ -z "${config}" ]]; then
        config="code"
    fi
    if [[ -z "${env}" ]]; then
        env="mysql"
    fi
    if [[ -z "${build}" ]]; then
        build="n"
    fi
    if [[ -z "${num}" ]]; then
        num=100
    fi
}

function setupSyncerConfig() {
    logi "Prepare syncer config"

    mkdir -p ${TEST_DATA_DIR}/config/consumer
    rm -f ${TEST_DATA_DIR}/config/consumer/*

    # env = mysql
    cp config/base/producer.yml data/config/producer.yml
    if [[ ${env} = "drds" ]]; then
        cp config/test-config/consumer-drds.yml data/config/consumer/consumer.yml
        cp config/test-config/producer-drds.yml data/config/producer.yml
    elif [[ ${env} = "mongo" ]]; then
        cp config/test-consumer/test-input/consumer-mongo.yml data/config/consumer/consumer.yml
        cp config/test-consumer/test-input/producer-mongo.yml data/config/producer.yml
    elif [[ ${config} = "yaml" || ${config} = "yml" ]]; then
        cp config/base/consumer.yml data/config/consumer/consumer.yml
    elif [[ ${config} = "code" ]]; then
        cp config/base/consumer-code.yml data/config/consumer/consumer-code.yml
    else
        loge "Invalid option: $config"
        exit 1
    fi
}

function buildSyncer() {
    if [[ ${build} != "n" ]]; then
        # package syncer
        cd ..
        mvn package

        # build syncer image
        #docker rmi -f syncer:test
        docker build syncer-core -t syncer:test
        cd test
    fi
}


setupDefaultPara
configEnvVar

logi "Using env=$env, build=$build, config=$config, num=$num"

buildSyncer
setupSyncerConfig


# Given
# start env by docker-compose
# init data
logi "Prepare $env runtime"
bash script/setupEnv.sh ${num} ${env}
bash script/loadData.sh ${num} ${env}


for (( i = 0; i < 3; ++i )); do
    printf "Waiting Syncer to warm up .\r"
    sleep 1
    printf "Waiting Syncer to warm up ..\r"
    sleep 1
    printf "Waiting Syncer to warm up ...\r"
    sleep 1
    echo ""
done


# Then
# query mysql/es count
for f in `find then -name "*.sh"` ; do
    bash ${f} ${env}
done
