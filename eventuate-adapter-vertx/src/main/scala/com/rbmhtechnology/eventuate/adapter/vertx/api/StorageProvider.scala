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

package com.rbmhtechnology.eventuate.adapter.vertx.api

import scala.concurrent.{ ExecutionContext, Future }

/**
 * A storage provider is used to persist the log read progress of individual `event producers`.
 */
trait StorageProvider {

  /**
   * Reads the log read progress of an `event producer` with the given id.
   *
   * @param id Id of the `event producer`.
   * @param executionContext Execution context to perform the operation.
   * @return Persisted sequence number for the given id.
   */
  def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[Long]

  /**
   * Persists the log read progress of an `event producer` with the given id.
   *
   * @param id Id of the `event producer`.
   * @param sequenceNr Sequence number to persist.
   * @param executionContext Execution context to perform the operation.
   * @return Persisted sequence number for the given id.
   */
  def writeProgress(id: String, sequenceNr: Long)(implicit executionContext: ExecutionContext): Future[Long]
}
