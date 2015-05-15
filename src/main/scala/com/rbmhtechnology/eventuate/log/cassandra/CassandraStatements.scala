/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.log.cassandra

private[eventuate] trait CassandraStatements {
  def config: CassandraConfig

  def createKeySpaceStatement = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${config.replicationFactor} }
    """

  def table(suffix: String): String =
    s"${config.keyspace}.${config.tablePrefix}_${suffix}"
}

private[eventuate] trait CassandraEventStatements extends CassandraStatements {
  def createEventTableStatement(logId: String) = s"""
      CREATE TABLE IF NOT EXISTS ${eventTable(logId)} (
        partition_nr bigint,
        sequence_nr bigint,
        marker text,
        eventBatch blob,
        PRIMARY KEY (partition_nr, sequence_nr, marker))
        WITH COMPACT STORAGE
    """

  def writeEventHeaderStatement(logId: String) = s"""
      INSERT INTO ${eventTable(logId)} (partition_nr, sequence_nr, marker, eventBatch)
      VALUES (?, 0, 'H', 0x00)
    """

  def writeEventBatchStatement(logId: String) = s"""
      INSERT INTO ${eventTable(logId)} (partition_nr, sequence_nr, marker, eventBatch)
      VALUES (?, ?, 'E', ?)
    """

  def readEventHeaderStatement(logId: String) = s"""
      SELECT * FROM ${eventTable(logId)} WHERE
        partition_nr = ? AND
        sequence_nr = 0
    """

  def readEventBatchesStatement(logId: String) = s"""
      SELECT * FROM ${eventTable(logId)} WHERE
        partition_nr = ? AND
        sequence_nr >= ?
      LIMIT ${config.maxResultSetSize}
    """

  def eventTable(logId: String) = table(logId)
}

private[eventuate] trait CassandraAggregateEventStatements extends CassandraStatements {
  def createAggregateEventTableStatement(logId: String) = s"""
      CREATE TABLE IF NOT EXISTS ${aggregateEventTable(logId)} (
        aggregate_id text,
        sequence_nr bigint,
        eventBatch blob,
        PRIMARY KEY (aggregate_id, sequence_nr))
        WITH COMPACT STORAGE
    """

  def writeAggregateEventBatchStatement(logId: String) = s"""
      INSERT INTO ${aggregateEventTable(logId)} (aggregate_id, sequence_nr, eventBatch)
      VALUES (?, ?, ?)
    """

  def readAggregateEventBatchesStatement(logId: String) = s"""
      SELECT * FROM ${aggregateEventTable(logId)} WHERE
        aggregate_id = ? AND
        sequence_nr >= ?
      LIMIT ${config.maxResultSetSize}
    """

  def aggregateEventTable(logId: String) = s"${table(logId)}_agg"
}

private[eventuate] trait CassandraSequenceNumberStatements extends CassandraStatements {
  def createSequenceNumberTableStatement = s"""
      CREATE TABLE IF NOT EXISTS ${sequenceNumberTable} (
        log_id text,
        sequence_nr bigint,
        PRIMARY KEY (log_id))
    """

  def writeSequenceNumberStatement = s"""
      INSERT INTO ${sequenceNumberTable} (log_id, sequence_nr)
      VALUES (?, ?)
    """

  def readSequenceNumberStatement = s"""
      SELECT * FROM ${sequenceNumberTable} WHERE
        log_id = ?
    """

  def sequenceNumberTable = table("snr")
}

private[eventuate] trait CassandraReplicationProgressStatements extends CassandraStatements {
  def createReplicationProgressTableStatement = s"""
      CREATE TABLE IF NOT EXISTS ${replicationProgressTable} (
        log_id text,
        source_log_id text,
        source_log_read_pos bigint,
        PRIMARY KEY (log_id, source_log_id))
    """

  def writeReplicationProgressStatement: String = s"""
      INSERT INTO ${replicationProgressTable} (log_id, source_log_id, source_log_read_pos)
      VALUES (?, ?, ?)
    """

  def readReplicationProgressStatement: String =  s"""
      SELECT * FROM ${replicationProgressTable} WHERE
        log_id = ?
    """

  def replicationProgressTable = table("rp")
}
