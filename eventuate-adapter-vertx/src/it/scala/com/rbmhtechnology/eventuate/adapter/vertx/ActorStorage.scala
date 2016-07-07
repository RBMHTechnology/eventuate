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

import akka.pattern.ask
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.rbmhtechnology.eventuate.adapter.vertx.api.StorageProvider
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait ActorStorage extends BeforeAndAfterEach {
  this: TestKit with Suite =>

  var storageProbe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    storageProbe = TestProbe()
  }

  def actorStorageProvider(): StorageProvider = new StorageProvider {
    implicit val timeout = Timeout(20.seconds)

    override def readProgress(logName: String)(implicit executionContext: ExecutionContext): Future[Long] =
      storageProbe.ref.ask(read(logName)).mapTo[Long]

    override def writeProgress(logName: String, sequenceNr: Long)(implicit executionContext: ExecutionContext): Future[Long] =
      storageProbe.ref.ask(write(logName)(sequenceNr)).mapTo[Long]
  }

  def read(logName: String): String =
    s"read[$logName]"

  def write(logName: String)(progress: Long): String =
    s"write[$logName]-$progress"
}
