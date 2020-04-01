#!/usr/bin/env bash

source cases/test-normal.sh

# shellcheck disable=SC2034
env=drds-es7

source "${UTIL_LIB}"

setup
test-non-latest
cleanup
