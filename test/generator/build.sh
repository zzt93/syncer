#!/usr/bin/env bash


cd test/generator
docker build . -f DataGenerator.Dockerfile -t generator:test
cd -