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

package com.rbmhtechnology.eventuate.crdt.japi

import java.util.concurrent.CompletionStage

import com.rbmhtechnology.eventuate.SnapshotMetadata

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

trait CRDTService[A, B, C] {
  import CRDTConverter._

  protected def delegate: com.rbmhtechnology.eventuate.crdt.CRDTService[A, B]

  implicit protected def c: CRDTConverter[B, C]

  /**
   * Starts the CRDT service.
   */
  def start(): Unit =
    delegate.start()

  /**
   * Stops the CRDT service.
   */
  def stop(): Unit =
    delegate.stop()

  /**
   * Returns the current value of the CRDT identified by `id`.
   */
  def value(id: String): CompletionStage[C] =
    delegate.value(id).asJava

  /**
   * Saves a snapshot of the CRDT identified by `id`.
   */
  def save(id: String): CompletionStage[SnapshotMetadata] =
    delegate.save(id).toJava
}

private[japi] case class CRDTConverter[A, B](f: A => B)(implicit e: ExecutionContext) {
  import scala.compat.java8.FutureConverters._

  def convert(ft: Future[A]): CompletionStage[B] =
    ft.map(f).toJava
}

private[japi] object CRDTConverter {
  case class CRDTConverterOps[A, B](ft: Future[A], c: CRDTConverter[A, B]) {
    def asJava: CompletionStage[B] = c.convert(ft)
  }

  implicit def toCRDTConverterOps[A, B](ft: Future[A])(implicit c: CRDTConverter[A, B]): CRDTConverterOps[A, B] =
    CRDTConverterOps(ft, c)
}
