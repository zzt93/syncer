#!/usr/bin/env bash
export BUILD_ID=dontKillMe
fuser -k -n tcp 5006
producer=$1
consumerList=$2
nohup java "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006" -server -XX:+UseParallelGC -jar ./target/syncer-1.0-SNAPSHOT.jar \
 --producerConfig=$1 --consumerConfig=consumerList &
