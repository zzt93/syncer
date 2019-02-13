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



# Given
# start env by docker-compose
# init data
if [[ $env = "mysql" ]]; then
    bash prepare_mysql.sh
elif [[ $env = "drds" ]]; then
    bash prepare_drds.sh
else
    echo "prepare mysql env"
    bash prepareMysql.sh
fi


t1=`echo 'select count(*) from test.news' | mysql -uroot -h localhost -proot -P43306`
# Then
# query ES count

c1=`curl -X GET "localhost:49200/*/_doc/_count" -H 'Content-Type: application/json'`

c2=`curl -X GET "localhost:49200/*/_doc/_count" -H 'Content-Type: application/json' -d'
{
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
'`

# query mysql count
d1=`echo 'select count(*) from test.news_bak' | mysql -uroot -h localhost -proot -P43306`
