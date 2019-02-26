#!/usr/bin/env bash
config=$1
env=$2
num=$3


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
    echo "Using config=$config, env=$env, num=$num"
}

function setupSyncerConfig() {
    echo "Prepare syncer config"

    mkdir -p data/config/consumer
    rm data/config/consumer/*


    if [[ ${env} = "drds" ]]; then
        cp config/test-config/consumer-drds.yml data/config/consumer/consumer.yml
    elif [[ ${config} = "yaml" || ${config} = "yml" ]]; then
        cp config/base/consumer.yml data/config/consumer/consumer.yml
    elif [[ ${config} = "code" ]]; then
        cp config/base/consumer-code.yml data/config/consumer/consumer-code.yml
    else
        echo "Invalid option: $config"
        exit 1
    fi
}


# package syncer
mvn package

# build syncer image
#docker rmi -f syncer:test
docker build syncer-core -t syncer:test

cd test


setupDefaultPara
setupSyncerConfig


# Given
# start env by docker-compose
# init data
echo "Prepare $env env"
bash setupEnv.sh ${num} ${env}


# Then
# query mysql/es count
export RED='\033[0;31m'
export NC='\033[0m' # No Color
for f in `find then -name "*.sh"` ; do
    bash ${f} ${env}
done
