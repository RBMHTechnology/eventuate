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

package com.rbmhtechnology.eventuate.snapshot.filesystem

import java.io.File

import akka.actor.ActorSystem

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.utilities._
import com.typesafe.config._

import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.collection.immutable.Seq

object FilesystemSnapshotStoreSpec {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 10s
      |
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
      |eventuate.snapshot.filesystem.snapshots-per-emitter-max = 3
    """.stripMargin)
}

class FilesystemSnapshotStoreSpec extends WordSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import FilesystemSnapshotStoreSpec._

  val system = ActorSystem("test", config)
  val settings = new FilesystemSnapshotStoreSettings(system)
  val store = new FilesystemSnapshotStore(settings, "a")

  var emitterIdCtr = 0

  override protected def beforeEach(): Unit =
    emitterIdCtr += 1

  override protected def afterAll(): Unit = {
    system.terminate().await
    FileUtils.deleteDirectory(new File(settings.rootDir))
  }

  def emitterId: String =
    emitterIdCtr.toString

  def snapshot(payload: String, sequenceNr: Long, emitterId: String = emitterId): Snapshot = {
    val event = DurableEvent.apply(emitterId).copy(localSequenceNr = sequenceNr)
    Snapshot(payload, emitterId, event, VectorTime("x" -> 17L))
  }

  def storedSequenceNrs(emitterId: String = emitterId): Seq[Long] =
    store.decreasingSequenceNrs(store.dstDir(emitterId))

  "A FilesystemSnapshotStore" must {
    "store a snapshot" in {
      store.saveAsync(snapshot("s1", 10L)).await
      storedSequenceNrs() should be(List(10L))
    }
    "store several snapshots" in {
      store.saveAsync(snapshot("s1", 10L)).await
      store.saveAsync(snapshot("s2", 11L)).await
      storedSequenceNrs() should be(List(11L, 10L))
    }
    "store several snapshots up to a configured maximum number" in {
      store.saveAsync(snapshot("s1", 10L)).await
      store.saveAsync(snapshot("s2", 11L)).await
      store.saveAsync(snapshot("s3", 12L)).await
      store.saveAsync(snapshot("s4", 13L)).await
      storedSequenceNrs() should be(List(13L, 12L, 11L))
    }
    "return None when loading a non-existing snapshot" in {
      store.loadAsync(emitterId).await should be(None)
    }
    "load a stored snapshot" in {
      store.saveAsync(snapshot("s1", 10L)).await
      store.loadAsync(emitterId).await should be(Some(snapshot("s1", 10L)))
    }
    "load the latest of several stored snapshots" in {
      store.saveAsync(snapshot("s1", 10L)).await
      store.saveAsync(snapshot("s2", 11L)).await
      store.loadAsync(emitterId).await should be(Some(snapshot("s2", 11L)))
    }
    "fallback to an older snapshot if loading fails" in {
      val dir = store.dstDir(emitterId)

      store.saveAsync(snapshot("s1", 10L)).await
      FileUtils.write(store.dstFile(dir, 11L), "blah")
      store.loadAsync(emitterId).await should be(Some(snapshot("s1", 10L)))
    }
    "return None when loading of all snapshots fails" in {
      val dir = store.dstDir(emitterId)

      FileUtils.write(store.dstFile(dir, 10L), "blah")
      FileUtils.write(store.dstFile(dir, 11L), "blah")
      FileUtils.write(store.dstFile(dir, 12L), "blah")
      store.loadAsync(emitterId).await should be(None)
    }
    "delete snapshots greater than or equal to a lower sequence number limit" in {
      val emitterA = s"${emitterId}-A"
      val emitterB = s"${emitterId}-B"

      store.saveAsync(snapshot("s1", 9L, emitterA)).await
      store.saveAsync(snapshot("s2", 11L, emitterA)).await
      store.saveAsync(snapshot("s3", 10L, emitterB)).await
      store.saveAsync(snapshot("s4", 13L, emitterB)).await
      store.deleteAsync(11L).await

      storedSequenceNrs(emitterA) should be(List(9L))
      storedSequenceNrs(emitterB) should be(List(10L))
    }
  }
}
