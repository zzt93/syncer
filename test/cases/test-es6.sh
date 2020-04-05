#!/usr/bin/env bash

source cases/test-es7.sh

# shellcheck disable=SC2034
env=drds-es6
num=100

source "${UTIL_LIB}"

setup
test-new-es
cleanup
