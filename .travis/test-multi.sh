#!/bin/sh

sbt $1 "multi-jvm:testOnly \
  com.rbmhtechnology.eventuate.BasicCausalitySpecLeveldb \
  com.rbmhtechnology.eventuate.BasicReplicationSpecLeveldb \
  com.rbmhtechnology.eventuate.BasicReplicationThroughputSpecLeveldb \
  com.rbmhtechnology.eventuate.FailureDetectionSpecLeveldb \
  com.rbmhtechnology.eventuate.FilteredReplicationSpecLeveldb \
  com.rbmhtechnology.eventuate.crdt.ReplicatedORSetSpecLeveldb"
