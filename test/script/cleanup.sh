#!/usr/bin/env bash

cd test
docker-compose -f docker-compose/drds.yml rm -fs
rm -rf data/*