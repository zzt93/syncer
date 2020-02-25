#!/usr/bin/env bash

source ./test-mongo-input.sh

env=mongo_v4
num=100
syncerDir=mongo-input

source ${UTIL_LIB}

setup
test-mongo-input
cleanup
