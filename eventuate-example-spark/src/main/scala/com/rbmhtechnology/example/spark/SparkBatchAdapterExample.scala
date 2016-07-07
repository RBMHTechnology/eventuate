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

//#spark-batch-adapter
import akka.actor.ActorSystem

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.adapter.spark.SparkBatchAdapter

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }

//#

import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLog

//#spark-batch-dataframe
import org.apache.spark.sql.{ Dataset, DataFrame, SQLContext }
//#

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

//#spark-batch-dataframe

case class DomainEvent(sequenceNr: Long, payload: String)
//#

object SparkBatchAdapterExample extends App {

  // ---------------------------------------------------------------
  //  Assumption: Cassandra 2.1 or higher running on localhost:9042
  // ---------------------------------------------------------------

  //#spark-batch-adapter
  implicit val system = ActorSystem("spark-example")

  val sparkConfig = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.connection.port", "9042")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
    .setAppName("adapter")
    .setMaster("local[4]")

  val logId = "example"
  //#

  val log = system.actorOf(CassandraEventLog.props("example"))
  // Write some events to event log
  Await.result(new EventLogWriter("writer", log).write(Seq("a", "b", "c", "d", "e")), 20.seconds)

  //#spark-batch-adapter
  val sparkContext: SparkContext =
    new SparkContext(sparkConfig)

  // Create an Eventuate Spark batch adapter
  val sparkBatchAdapter: SparkBatchAdapter =
    new SparkBatchAdapter(sparkContext, system.settings.config)

  // Expose all events of given event log as Spark RDD
  val events: RDD[DurableEvent] =
    sparkBatchAdapter.eventBatch(logId)

  // Expose events of given event log as Spark RDD, starting at sequence number 3
  val eventsFrom: RDD[DurableEvent] =
    sparkBatchAdapter.eventBatch(logId, fromSequenceNr = 3L)
  //#

  //#spark-batch-sorting
  // By default, events are sorted by sequence number *per partition*.
  // Use .sortBy(_.localSequenceNr) to create a totally ordered RDD.
  val eventsSorted: RDD[DurableEvent] = events.sortBy(_.localSequenceNr)
  //#

  //#spark-batch-dataframe

  val sqlContext: SQLContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

  // Create a DataFrame from RDD[DurableEvent]
  val eventsDF: DataFrame = events.map(event =>
    DomainEvent(event.localSequenceNr, event.payload.toString)).toDF()

  // Create a Dataset from RDD[DurableEvent]
  val eventDS: Dataset[DomainEvent] = events.map(event =>
    DomainEvent(event.localSequenceNr, event.payload.toString)).toDS()
  //#

  // Write sorted events to stdout
  eventsSorted.collect().foreach(println)

  // Sorted DataFrame by sequenceNr and write to stdout
  eventsDF.orderBy("sequenceNr").collect().foreach(println)

  sparkContext.stop()
  system.terminate()
}
