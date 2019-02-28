#!/usr/bin/env bash
set -e

config=$1
env=$2
num=$3


function configEnv() {
    export LOG_LIB=`pwd`/log.sh
    source ${LOG_LIB}
    export ENV_CONFIG=`pwd`/${env}.yml
}

function setupDefaultPara() {
    if [[ -z "${config}" ]]; then
        config="code"
    fi
    if [[ -z "${env}" ]]; then
        env="mysql"
    fi
    if [[ -z "${num}" ]]; then
        num=100
    fi
    logi "Using config=$config, env=$env, num=$num"
}

function setupSyncerConfig() {
    logi "Prepare syncer config"

    mkdir -p data/config/consumer
    rm data/config/consumer/*

    cp config/base/producer.yml data/config/producer.yml
    if [[ ${env} = "drds" ]]; then
        cp config/test-config/consumer-drds.yml data/config/consumer/consumer.yml
        cp config/test-config/producer-drds.yml data/config/producer.yml
    elif [[ ${config} = "yaml" || ${config} = "yml" ]]; then
        cp config/base/consumer.yml data/config/consumer/consumer.yml
    elif [[ ${config} = "code" ]]; then
        cp config/base/consumer-code.yml data/config/consumer/consumer-code.yml
    else
        loge "Invalid option: $config"
        exit 1
    fi
}


# package syncer
mvn package

# build syncer image
#docker rmi -f syncer:test
docker build syncer-core -t syncer:test

cd test

configEnv
setupDefaultPara
setupSyncerConfig


# Given
# start env by docker-compose
# init data
logi "Prepare $env env"
bash setupEnv.sh ${num} ${env}


# Then
# query mysql/es count
for f in `find then -name "*.sh"` ; do
    bash ${f} ${env}
done
