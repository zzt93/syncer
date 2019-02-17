#!/usr/bin/env bash
env=$1
config=$2

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
if [[ $config = "yaml" ]]; then
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
if [[ $env = "mysql" ]]; then
    bash setup.sh 100 $env
elif [[ $env = "drds" ]]; then
    bash setup.sh 100 $env
else
    echo "prepare mysql env"
    bash setup.sh 100 $env
fi


t1=`echo 'select count(*) from test.news' | mysql -uroot -h localhost -proot -P43306`
echo "test.news: $t1"
# Then
# query ES count

c1=`curl -s -X GET "localhost:49200/*/_doc/_count" -H 'Content-Type: application/json'`
echo "es: $c1"

c2=`curl s- -X GET "localhost:49200/*/_doc/_count" -H 'Content-Type: application/json' -d'
{
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
'`

# query mysql count
d1=`echo 'select count(*) from test.news_bak' | mysql -uroot -h localhost -proot -P43306`
echo "test.news_bak: $c1"

