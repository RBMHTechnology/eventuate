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

package com.rbmhtechnology.example.querydb

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import com.datastax.driver.core._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util._

object WriterApp extends App {

  // ---------------------------------------------------------------
  //  Assumption: Cassandra 2.1 or higher running on localhost:9042
  // ---------------------------------------------------------------

  withQueryDB(drop = false) { session =>
    val system = ActorSystem("example-querydb", ConfigFactory.load(args(0)))
    val log = system.actorOf(LeveldbEventLog.props("example"))

    val emitter = system.actorOf(Props(new Emitter("emitter", log)))
    val writer = system.actorOf(Props(new Writer("writer", log, session)))

    import system.dispatcher

    implicit val timeout = Timeout(5.seconds)

    emitter ! CreateCustomer("Martin", "Krasser", "Somewhere 1")
    emitter ? CreateCustomer("Volker", "Stampa", "Somewhere 2") onComplete {
      case Success(CustomerCreated(cid, _, _, _)) => emitter ! UpdateAddress(cid, s"Somewhere ${Random.nextInt(10)}")
      case Failure(e)                             => e.printStackTrace()
    }

    Thread.sleep(3000)
    system.terminate()
  }

  def createQueryDB(drop: Boolean): Session = {
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()

    if (drop) {
      session.execute("DROP KEYSPACE IF EXISTS QUERYDB")
    }

    session.execute("CREATE KEYSPACE IF NOT EXISTS QUERYDB WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute("USE QUERYDB")

    session.execute("CREATE TABLE IF NOT EXISTS CUSTOMER (id bigint, first text, last text, address text, PRIMARY KEY (id))")
    session.execute("CREATE TABLE IF NOT EXISTS PROGRESS (id bigint, sequence_nr bigint, PRIMARY KEY (id))")
    session.execute("INSERT INTO PROGRESS (id, sequence_nr) VALUES(0, 0) IF NOT EXISTS")

    session
  }

  def dropQueryDB(session: Session): Unit = {
    session.close()
    session.getCluster.close()
  }

  def withQueryDB[A](drop: Boolean = true)(f: Session => A): A = {
    val session = createQueryDB(drop)
    try f(session) finally dropQueryDB(session)
  }
}
