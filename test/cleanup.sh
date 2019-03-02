#!/usr/bin/env bash

cd test
docker-compose -f mysql.yml rm -fs
docker-compose -f drds.yml rm -fs
rm -rf data/*