#!/usr/bin/env bash

logi(){
  echo "[$(date +'%Y-%m-%d %H:%M:%S%z')] info: $@"
}
loge() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S%z')] error: $@" >&2
}