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

import java.io._
import java.net.URLEncoder

import akka.event.{LogSource, Logging}
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.snapshot.SnapshotStore
import org.apache.commons.io.IOUtils

import scala.concurrent.Future
import scala.collection.immutable.Seq
import scala.util._

object FilesystemSnapshotStore {
  implicit val logSource = LogSource.fromAnyClass[FilesystemSnapshotStore]
}

/**
 * A snapshot store that saves snapshots to the local filesystem. It keeps a configurable
 * maximum number of snapshots per event-sourced actor or view. Snapshot loading falls
 * back to older snapshots if newer snapshots cannot be loaded.
 *
 * @see Configuration key `eventuate.snapshot.filesystem.snapshots-per-emitter-max`.
 */
class FilesystemSnapshotStore(settings: FilesystemSnapshotStoreSettings, logId: String) extends SnapshotStore {
  private val log = Logging(settings.system, classOf[FilesystemSnapshotStore])
  private val rootDir = new File(settings.rootDir, URLEncoder.encode(logId, "UTF-8"))

  rootDir.mkdirs()

  override def deleteAsync(lowerSequenceNr: Long): Future[Unit] = {
    import settings.writeDispatcher
    Future(delete(lowerSequenceNr))
  }

  override def saveAsync(snapshot: Snapshot): Future[Unit] = {
    import settings.writeDispatcher
    Future(withOutputStream(dstDir(snapshot.metadata.emitterId), snapshot.metadata.sequenceNr)(serialize(_, snapshot)))
  }

  override def loadAsync(emitterId: String): Future[Option[Snapshot]] = {
    import settings.readDispatcher
    Future(load(dstDir(emitterId)))
  }

  def delete(lowerSequenceNr: Long): Unit = for {
    emitterId  <- rootDir.listFiles
    emitterDir  = dstDir(emitterId.getName)
    sequenceNr <- decreasingSequenceNrs(emitterDir) if sequenceNr >= lowerSequenceNr
  } dstFile(emitterDir, sequenceNr).delete()

  def load(dir: File): Option[Snapshot] = {
    @annotation.tailrec
    def go(snrs: Seq[Long]): Option[Snapshot] = snrs.headOption match {
      case None => None
      case Some(snr) => Try(withInputStream(dir, snr)(deserialize)) match {
        case Success(s) => Some(s)
        case Failure(e) =>
          log.error(s"error loading snapshot ${dstFile(dir, snr)}")
          go(snrs.tail)
      }
    }
    go(decreasingSequenceNrs(dir))
  }

  private def serialize(outputStream: OutputStream, snapshot: Snapshot): Unit =
    outputStream.write(settings.serialization.serialize(snapshot).get)

  private def deserialize(inputStream: InputStream): Snapshot =
    settings.serialization.deserialize(IOUtils.toByteArray(inputStream), classOf[Snapshot]).get

  private def withOutputStream(dir: File, snr: Long)(body: OutputStream => Unit): Unit = {
    val dst = dstFile(dir, snr)
    val tmp = tmpFile(dir, snr)

    dir.mkdirs()
    withStream(new BufferedOutputStream(new FileOutputStream(tmp)), body)
    tmp.renameTo(dst)

    // do not keep more than the configured maximum number of snapshot files
    decreasingSequenceNrs(dir).drop(settings.snapshotsPerEmitterMax).foreach { snr =>
      dstFile(dir, snr).delete()
    }
  }

  private def withInputStream[A](dir: File, snr: Long)(body: InputStream => A): A =
    withStream(new BufferedInputStream(new FileInputStream(dstFile(dir, snr))), body)

  private def withStream[A <: Closeable, B](stream: A, p: A => B): B =
    try { p(stream) } finally { stream.close() }

  private val DstFilenamePattern =
    """^snr-(\d+)""".r

  private[eventuate] def dstDir(emitterId: String): File =
    new File(rootDir, URLEncoder.encode(emitterId, "UTF-8"))

  private[eventuate] def dstFile(dstDir: File, sequenceNr: Long): File =
    new File(dstDir, s"snr-${sequenceNr}")

  private[eventuate] def tmpFile(dstDir: File, sequenceNr: Long): File =
    new File(dstDir, s"tmp-${sequenceNr}")

  private[eventuate] def decreasingSequenceNrs(dir: File): Seq[Long] =
    if (!dir.exists) Nil else dir.listFiles.map(_.getName).collect { case DstFilenamePattern(snr) => snr.toLong }.toList.sorted.reverse
}
