/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.eventuate

object ReplicationConnection {
  /**
   * Default name of the remote actor system to connect to.
   */
  val DefaultRemoteSystemName: String = "location"

  /**
   * Creates [[ReplicationConnection]] with remote actor system `name` set to [[DefaultRemoteSystemName]].
   *
   * @param host host of the remote actor system that runs a [[ReplicationEndpoint]].
   * @param port port of the remote actor system that runs a [[ReplicationEndpoint]].
   * @param filters Replication filters applied remotely. Filters are applied to individual
   *                event logs where filter keys are the corresponding event log names.
   */
  def apply(host: String, port: Int, filters: Map[String, ReplicationFilter]): ReplicationConnection =
    new ReplicationConnection(host, port, filters = filters)

  /**
   * Creates [[ReplicationConnection]] with remote actor system `name` set to [[DefaultRemoteSystemName]]
   * and an optional replication `filter` applied to the event log with name [[ReplicationEndpoint.DefaultLogName]].
   *
   * @param host host of the remote actor system that runs a [[ReplicationEndpoint]].
   * @param port port of the remote actor system that runs a [[ReplicationEndpoint]].
   * @param filter Optional filter applied remotely. If defined, the filter is applied to
   *               the event log with name [[ReplicationEndpoint.DefaultLogName]].
   */
  def apply(host: String, port: Int, filter: Option[ReplicationFilter]): ReplicationConnection = filter match {
    case Some(f) => new ReplicationConnection(host, port, filters = Map(ReplicationEndpoint.DefaultLogName -> f))
    case None    => new ReplicationConnection(host, port)
  }
}

/**
 * A replication connection descriptor.
 *
 * @param host Host of the remote actor system that runs a [[ReplicationEndpoint]].
 * @param port Port of the remote actor system that runs a [[ReplicationEndpoint]].
 * @param name Name of the remote actor system that runs a [[ReplicationEndpoint]].
 * @param filters Replication filters applied remotely. Filters are applied to individual
 *                event logs where filter keys are the corresponding event log names.
 */
case class ReplicationConnection(host: String, port: Int, name: String = ReplicationConnection.DefaultRemoteSystemName, filters: Map[String, ReplicationFilter] = Map.empty)
