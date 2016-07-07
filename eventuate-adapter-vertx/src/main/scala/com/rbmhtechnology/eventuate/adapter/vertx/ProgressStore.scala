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

package com.rbmhtechnology.eventuate.adapter.vertx

import com.rbmhtechnology.eventuate.adapter.vertx.api.StorageProvider

import scala.concurrent.{ ExecutionContext, Future }

trait ProgressStore[R, W] {
  def writeProgress(id: String, snr: Long)(implicit executionContext: ExecutionContext): Future[W]

  def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[R]

  def progress(result: R): Long
}

trait SequenceNumberProgressStore extends ProgressStore[Long, Long] {
  def storageProvider: StorageProvider

  override def writeProgress(id: String, snr: Long)(implicit executionContext: ExecutionContext): Future[Long] =
    storageProvider.writeProgress(id, snr)

  override def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[Long] =
    storageProvider.readProgress(id)

  override def progress(result: Long): Long =
    result
}
