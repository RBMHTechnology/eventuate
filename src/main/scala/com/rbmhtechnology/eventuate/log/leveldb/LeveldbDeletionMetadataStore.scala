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

package com.rbmhtechnology.eventuate.log.leveldb

import java.nio.ByteBuffer

import com.rbmhtechnology.eventuate.log.DeletionMetadata
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog.WithBatch
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog.longBytes
import org.iq80.leveldb.DB
import org.iq80.leveldb.WriteOptions

class LeveldbDeletionMetadataStore(val leveldb: DB, val leveldbWriteOptions: WriteOptions, classifier: Int) extends WithBatch {
  private val DeletedToSequenceNrKey: Int = 1
  private val RemoteLogIdsKey: Int = 2

  private val StringSetSeparatorChar = '\u0000'

  def writeDeletionMetadata(info: DeletionMetadata): Unit = withBatch { batch =>
    batch.put(idKeyBytes(DeletedToSequenceNrKey), longBytes(info.toSequenceNr))
    batch.put(idKeyBytes(RemoteLogIdsKey), stringSetBytes(info.remoteLogIds))
  }

  def readDeletionMetadata(): DeletionMetadata = {
    val toSequenceNr = longFromBytes(leveldb.get(idKeyBytes(DeletedToSequenceNrKey)))
    val remoteLogIds = stringSetFromBytes(leveldb.get(idKeyBytes(RemoteLogIdsKey)))
    DeletionMetadata(toSequenceNr, remoteLogIds)
  }

  private def idKeyBytes(key: Int): Array[Byte] = {
    val bb = ByteBuffer.allocate(8)
    bb.putInt(classifier)
    bb.putInt(key)
    bb.array
  }

  private def longFromBytes(longBytes: Array[Byte]): Long =
    if (longBytes == null) 0 else LeveldbEventLog.longFromBytes(longBytes)

  private def stringSetBytes(set: Set[String]): Array[Byte] =
    set.mkString(StringSetSeparatorChar.toString).getBytes("UTF-8")

  private def stringSetFromBytes(setBytes: Array[Byte]): Set[String] =
    if (setBytes == null || setBytes.length == 0)
      Set.empty
    else
      new String(setBytes, "UTF-8").split(StringSetSeparatorChar).toSet
}
