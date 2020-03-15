#!/usr/bin/env bash

source cases/test-mongo-input.sh

env=mongo_v4
num=200
syncerDir=mongo-input

source ${UTIL_LIB}

setup
test-mongo-input
cleanup
