#!/usr/bin/env bash

cd test
docker-compose -f docker-compose/drds.yml rm -fvs
docker-compose -f docker-compose/mongo.yml rm -fvs
docker-compose -f docker-compose/mongo_v4.yml rm -fvs
rm -rf data/*