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

package com.rbmhtechnology.example.spark

//#spark-stream-adapter
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.adapter.spark.SparkStreamAdapter

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

//#

import akka.actor._

import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

import scala.collection.immutable._
import scala.io.Source

object SparkStreamAdapterExample extends App {
  implicit val system: ActorSystem = ActorSystem(ReplicationConnection.DefaultRemoteSystemName)

  val logName: String = "L"
  val endpoint: ReplicationEndpoint = new ReplicationEndpoint(id = "1", logNames = Set(logName), logFactory = logId => LeveldbEventLog.props(logId), connections = Set())
  val log: ActorRef = endpoint.logs(logName)
  val writer: EventLogWriter = new EventLogWriter("writer", log)

  endpoint.activate()

  //#spark-stream-adapter
  val sparkConfig = new SparkConf(true)
    .setAppName("adapter")
    .setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConfig)
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))

  // Create an Eventuate Spark stream adapter
  val sparkStreamAdapter = new SparkStreamAdapter(
    sparkStreamingContext, system.settings.config)

  // Create a DStream from event log L by connecting to its replication endpoint
  val stream: DStream[DurableEvent] = sparkStreamAdapter.eventStream(
    id = "s1", host = "127.0.0.1", port = 2552, logName = "L",
    fromSequenceNr = 1L, storageLevel = StorageLevel.MEMORY_ONLY)

  // For processing in strict event storage order, use repartition(1)
  stream.repartition(1).foreachRDD(rdd => rdd.foreach(println))

  // Start event stream processing
  sparkStreamingContext.start()
  //#

  // Generate new events from stdin
  val lines = Source.stdin.getLines()
  def prompt(): Unit = {
    if (lines.hasNext) lines.next() match {
      case "exit" =>
        sparkStreamingContext.stop(stopSparkContext = true)
        system.terminate()
      case line =>
        writer.write(Seq(line))
        prompt()
    }
  }
  prompt()
}
