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

import akka.actor.Props
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec

import org.cassandraunit.utils.EmbeddedCassandraServerHelper

trait CassandraEventLogMultiNodeSupport { this: MultiNodeSpec =>
  def coordinator: RoleName

  def logProps(logId: String): Props =
    CassandraEventLog.props(logId)

  override def atStartup(): Unit = {
    if (isNode(coordinator)) {
      EmbeddedCassandraServerHelper.startEmbeddedCassandra(60000)
      Cassandra(system)
    }
    enterBarrier("startup")
  }

  override def afterTermination(): Unit = {
    if (isNode(coordinator)) EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
}