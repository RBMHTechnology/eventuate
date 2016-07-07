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

package com.rbmhtechnology.eventuate.adapter.vertx.japi.rx

import java.lang.{ Long => JLong }

import com.rbmhtechnology.eventuate.adapter.vertx.api.{ StorageProvider => SStorageProvider }
import rx.{ Observable, Observer }

import scala.concurrent.{ ExecutionContext, Future, Promise }

/**
 * A storage provider is used to persist the log read progress of individual `event producers`.
 */
trait StorageProvider {

  /**
   * Reads the log read progress of an `event producer` with the given id.
   *
   * @param id Id of the `event producer`.
   * @return Persisted sequence number for the given id.
   */
  def readProgress(id: String): Observable[JLong]

  /**
   * Persists the log read progress of an `event producer` with the given id.
   *
   * @param id Id of the `event producer`.
   * @param sequenceNr Sequence number to persist.
   * @return Persisted sequence number for the given id.
   */
  def writeProgress(id: String, sequenceNr: JLong): Observable[JLong]
}

object StorageProvider {

  private def futureObserver[A](p: Promise[A]): Observer[A] =
    new Observer[A] {
      override def onNext(v: A): Unit = {
        if (!p.isCompleted) {
          p.success(v)
        }
      }

      override def onError(e: Throwable): Unit = {
        p.failure(e)
      }

      override def onCompleted(): Unit = {
        if (!p.isCompleted) {
          p.failure(new IllegalStateException("No item emitted by Observable"))
        }
      }
    }

  implicit class StorageProviderConverter(delegate: StorageProvider) {
    def asScala: SStorageProvider = new SStorageProvider {
      override def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[Long] = {
        val p = Promise[JLong]
        delegate.readProgress(id).subscribe(futureObserver(p))
        p.future.map(Long2long)
      }

      override def writeProgress(id: String, sequenceNr: Long)(implicit executionContext: ExecutionContext): Future[Long] = {
        val p = Promise[JLong]
        delegate.writeProgress(id, sequenceNr).subscribe(futureObserver(p))
        p.future.map(Long2long)
      }
    }
  }
}
