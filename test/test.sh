#!/usr/bin/env bash
config=$1
env=$2
num=$3


if [[ -z "${config}" ]]; then
    config="yaml"
fi
if [[ -z "${env}" ]]; then
    env="mysql"
fi
if [[ -z "${num}" ]]; then
    num=100
fi


# package syncer
mvn package

# build syncer image
#docker rmi -f syncer:test
docker build syncer-core -t syncer:test

###############33333

cd test

# add syncer config according to test case
cd data
mkdir -p config/consumer
rm config/consumer/*
if [[ $config = "yaml" || ${config} = "yml" ]]; then
    cp config/correctness-consumer.yml config/consumer/correctness-consumer.yml
elif [[ $config = "code" ]]; then
    cp config/correctness-consumer-code.yml config/consumer/correctness-consumer-code.yml
else
    echo ""
    exit 1
fi
cd ..


# Given
# start env by docker-compose
# init data
echo "prepare $env env"
bash setup.sh ${num} ${env}


# Then
# query mysql/es count
for f in `find then -name "*.sh"` ; do
    bash ${f} ${env}
done
