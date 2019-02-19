#!/usr/bin/env bash
config=$1
env=$2
num=$3

if [[ -z "${num}" ]]; then
    num=100
fi

if [[ -z "${env}" ]]; then
    env="mysql"
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
    echo "prepare code env"
    cp config/correctness-consumer-code.yml config/consumer/correctness-consumer-code.yml
fi
cd ..


# Given
# start env by docker-compose
# init data
echo "prepare $env env"
bash setup.sh $num $env


all=`docker-compose -f mysql.yml exec mysql mysql -uroot -proot -N -B -e 'select count(*) from test.news' | grep -o "[0-9]*"`
echo "test.news: $all"

# Then
# query ES count

c1=`curl -s -X GET "localhost:49200/test/news/_count" -H 'Content-Type: application/json'`
echo "es: $c1"
if [[ ${c1} -eq "$all" ]];then
    exit 1
fi

c2=`curl -s -X GET "localhost:49200/test/news/_count" -H 'Content-Type: application/json' -d'
{
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
'`

# query mysql count
d1=`docker-compose -f mysql.yml exec mysql mysql -uroot -proot -N -B -e 'select count(*) from test.news_bak' | grep -o "[0-9]*"`
echo "test.news_bak: $d1"

if [[ ${d1} -eq "$all" ]];then
    exit 1
fi