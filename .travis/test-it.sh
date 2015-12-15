#!/bin/sh

sbt $1 "it:testOnly \
  com.rbmhtechnology.eventuate.RecoverySpec \
  com.rbmhtechnology.eventuate.DeleteEventsSpec \
  com.rbmhtechnology.eventuate.crdt.* \
  com.rbmhtechnology.eventuate.serializer.* \
  com.rbmhtechnology.eventuate.snapshot.filesystem.* \
  *Leveldb"