#!/usr/bin/env bash

num=$1

cd generator

docker build . -f DataGenerator.Dockerfile -t generator:test
docker run -v data/:/data/ generator:test mysql_init.sql $1
docker run -v data/:/data/ generator:test mysql_simple.sql $1
