/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.log.cassandra

private[eventuate] trait CassandraStatements {
  def settings: CassandraEventLogSettings

  def createKeySpaceStatement = s"""
      CREATE KEYSPACE IF NOT EXISTS ${settings.keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${settings.replicationFactor} }
    """

  def table(suffix: String): String =
    s"${settings.keyspace}.${settings.tablePrefix}_${suffix}"
}

private[eventuate] trait CassandraEventStatements extends CassandraStatements {
  def createEventTableStatement(logId: String) = s"""
      CREATE TABLE IF NOT EXISTS ${eventTable(logId)} (
        partition_nr bigint,
        sequence_nr bigint,
        event blob,
        PRIMARY KEY (partition_nr, sequence_nr))
        WITH COMPACT STORAGE
    """

  def writeEventStatement(logId: String) = s"""
      INSERT INTO ${eventTable(logId)} (partition_nr, sequence_nr, event)
      VALUES (?, ?, ?)
    """

  def readEventsStatement(logId: String) = s"""
      SELECT * FROM ${eventTable(logId)} WHERE
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
      LIMIT ${settings.partitionSize}
    """

  def eventTable(logId: String) = table(logId)
}

private[eventuate] trait CassandraAggregateEventStatements extends CassandraStatements {
  def createAggregateEventTableStatement(logId: String) = s"""
      CREATE TABLE IF NOT EXISTS ${aggregateEventTable(logId)} (
        aggregate_id text,
        sequence_nr bigint,
        event blob,
        PRIMARY KEY (aggregate_id, sequence_nr))
        WITH COMPACT STORAGE
    """

  def writeAggregateEventStatement(logId: String) = s"""
      INSERT INTO ${aggregateEventTable(logId)} (aggregate_id, sequence_nr, event)
      VALUES (?, ?, ?)
    """

  def readAggregateEventsStatement(logId: String) = s"""
      SELECT * FROM ${aggregateEventTable(logId)} WHERE
        aggregate_id = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
      LIMIT ${settings.partitionSize}
    """

  def aggregateEventTable(logId: String) = s"${table(logId)}_agg"
}

private[eventuate] trait CassandraEventLogClockStatements extends CassandraStatements {
  def createEventLogClockTableStatement = s"""
      CREATE TABLE IF NOT EXISTS ${eventLogClockTable} (
        log_id text,
        clock blob,
        PRIMARY KEY (log_id))
    """

  def writeEventLogClockStatement = s"""
      INSERT INTO ${eventLogClockTable} (log_id, clock)
      VALUES (?, ?)
    """

  def readEventLogClockStatement = s"""
      SELECT * FROM ${eventLogClockTable} WHERE
        log_id = ?
    """

  def eventLogClockTable = table("elc")
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

  def readReplicationProgressStatement: String = s"""
      SELECT * FROM ${replicationProgressTable} WHERE
        log_id = ? AND
        source_log_id = ?
    """

  def readReplicationProgressesStatement: String = s"""
      SELECT * FROM ${replicationProgressTable} WHERE
        log_id = ?
    """

  def replicationProgressTable = table("rp")
}

private[eventuate] trait CassandraDeletedToStatements extends CassandraStatements {
  def createDeletedToTableStatement = s"""
      CREATE TABLE IF NOT EXISTS ${deletedToTable} (
        log_id text,
        deleted_to bigint,
        PRIMARY KEY (log_id))
    """

  def writeDeletedToStatement: String = s"""
      INSERT INTO ${deletedToTable} (log_id, deleted_to)
      VALUES (?, ?)
    """

  def readDeletedToStatement: String = s"""
      SELECT deleted_to FROM ${deletedToTable}
      WHERE log_id = ?
    """

  def deletedToTable = table("del")
}
