#!/usr/bin/env bash


export RED='\033[0;31m'
export NC='\033[0m' # No Color

logi(){
  echo "[$(date +'%Y-%m-%d %H:%M:%S%z')] info: $@"
}
loge() {
  echo -e "[$(date +'%Y-%m-%d %H:%M:%S%z')] ${RED}error${NC}: $@" >&2
}