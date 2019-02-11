#!/usr/bin/env bash

mvn package

docker build

docker-compose up

# Given
# init data

# Then
# query ES count

c1=`curl -X GET "localhost:9200/*/_doc/_count" -H 'Content-Type: application/json'`

c2=`curl -X GET "localhost:9200/*/_doc/_count" -H 'Content-Type: application/json' -d'
{
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
'`

