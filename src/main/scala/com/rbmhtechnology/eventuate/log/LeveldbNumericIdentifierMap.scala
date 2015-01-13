/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate.log

import java.nio.ByteBuffer

import akka.actor.Actor

import org.iq80.leveldb.DBIterator

trait LeveldbNumericIdentifierMap extends Actor { this: LeveldbEventLog =>
  import LeveldbNumericIdentifierMap._

  private var idMap: Map[String, Int] = Map.empty

  def numericId(id: String): Int = idMap.get(id) match {
    case None    => writeIdMapping(id, idMap.size)
    case Some(v) => v
  }

  private def readIdMap(): Map[String, Int] = withIterator { iter =>
    iter.seek(idKeyBytes(0))
    readIdMap(Map.empty, iter)
  }

  private def readIdMap(idMap: Map[String, Int], iter: DBIterator): Map[String, Int] = {
    if (!iter.hasNext) idMap else {
      val nextEntry = iter.next()
      val nextKey = idKey(nextEntry.getKey)
      if (nextKey == idKeyEnd) idMap else {
        val nextVal = new String(nextEntry.getValue, "UTF-8")
        readIdMap(idMap + (nextVal -> nextKey), iter)
      }
    }
  }

  private def writeIdMapping(id: String, nid: Int): Int = {
    idMap = idMap + (id -> nid)
    leveldb.put(idKeyBytes(nid), id.getBytes("UTF-8"))
    nid
  }

  override def preStart() {
    leveldb.put(idKeyEndBytes, Array.empty[Byte])
    idMap = readIdMap()
    super.preStart()
  }
}

object LeveldbNumericIdentifierMap {
  val idKeyEnd: Int = Int.MaxValue
  val idKeyEndBytes: Array[Byte] = {
    val bb = ByteBuffer.allocate(12)
    bb.putLong(-2L)
    bb.putInt(idKeyEnd)
    bb.array
  }

  def idKeyBytes(nid: Int): Array[Byte] = {
    val bb = ByteBuffer.allocate(12)
    bb.putLong(-2L)
    bb.putInt(nid)
    bb.array
  }

  def idKey(a: Array[Byte]): Int = {
    val bb = ByteBuffer.wrap(a)
    bb.getLong
    bb.getInt
  }
}

