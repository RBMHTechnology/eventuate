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

package com.rbmhtechnology.example

import akka.actor._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.VersionedObjects._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.typesafe.config.ConfigFactory

class OrderExample(manager: ActorRef, view: ActorRef) extends Actor {
  import OrderManager._
  import OrderView._

  val lines = io.Source.stdin.getLines

  def receive = {
    case GetStateSuccess(state) =>
      state.values.foreach(printOrder)
      prompt()
    case GetUpdateCountSuccess(orderId, count) =>
      println(s"[${orderId}] update count = ${count}")
      prompt()
    case CommandSuccess(_) =>
      prompt()
    case CommandFailure(_, cause: ConflictDetectedException[Order]) =>
      println(s"${cause.getMessage}, select one of the following versions to resolve conflict")
      printOrder(cause.versions)
      prompt()
    case CommandFailure(_, cause) =>
      println(cause.getMessage)
      prompt()
    case line: String => line.split(' ').toList match {
      case "state"                 :: Nil => manager ! GetState
      case "count"   :: id         :: Nil => view    ! GetUpdateCount(id)
      case "create"  :: id         :: Nil => manager ! CreateOrder(id)
      case "cancel"  :: id         :: Nil => manager ! CancelOrder(id)
      case "add"     :: id :: item :: Nil => manager ! AddOrderItem(id, item)
      case "remove"  :: id :: item :: Nil => manager ! RemoveOrderItem(id, item)
      case "resolve" :: id :: idx  :: Nil => manager ! Resolve(id, idx.toInt)
      case       Nil => prompt()
      case "" :: Nil => prompt()
      case na :: nas => println(s"unknown command: ${na}"); prompt()
    }
  }

  def prompt(): Unit = {
    if (lines.hasNext) lines.next() match {
      case "exit" => context.system.shutdown()
      case line   => self ! line
    }
  }

  override def preStart(): Unit =
    prompt()
}

object OrderExample extends App {
  val cc = ConfigFactory.load("common.conf")
  val sc = ConfigFactory.load(args(0))

  val system = ActorSystem(ReplicationConnection.DefaultRemoteSystemName, sc.withFallback(cc))
  val endpoint = ReplicationEndpoint(id => LeveldbEventLog.props(id, "scala"))(system)
  val manager = system.actorOf(Props(new OrderManager(endpoint.id, endpoint.logs(ReplicationEndpoint.DefaultLogName))))
  val view = system.actorOf(Props(new OrderView(s"${endpoint.id}-view", endpoint.logs(ReplicationEndpoint.DefaultLogName))))
  val driver = system.actorOf(Props(new OrderExample(manager, view)).withDispatcher("cli-dispatcher"))
}

