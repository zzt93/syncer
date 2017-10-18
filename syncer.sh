#!/usr/bin/env bash
export BUILD_ID=dontKillMe
fuser -k -n tcp 5006
nohup java "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006" -jar ./target/syncer-1.0-SNAPSHOT.jar --pipelineConfig=$1 &