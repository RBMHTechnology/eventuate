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

package doc

trait LocalEventLogLeveldbDoc {
  //#local-log-leveldb
  import akka.actor.ActorSystem
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

  val system: ActorSystem = // ...
  //#
    ActorSystem("location")

  //#local-log-leveldb
  val log = system.actorOf(LeveldbEventLog.props(logId = "L1", prefix = "log"))
  //#
}

trait LocalEventLogCassandraDoc {
  //#local-log-cassandra
  import akka.actor.ActorSystem
  import com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLog

  val system: ActorSystem = // ...
  //#
    ActorSystem("location")

  //#local-log-cassandra
  val log = system.actorOf(CassandraEventLog.props(logId = "L1"))
  //#
}

trait ReplicationEndpointDoc {
  import akka.actor._
  //#replication-endpoint-1
  import com.rbmhtechnology.eventuate._
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

  implicit val system: ActorSystem =
    ActorSystem(ReplicationConnection.DefaultRemoteSystemName)

  val endpoint1 = new ReplicationEndpoint(id = "1", logNames = Set("L", "M"),
    logFactory = logId => LeveldbEventLog.props(logId),
    connections = Set(ReplicationConnection("127.0.0.1", 2553)))

  endpoint1.activate()
  //#

  //#logs-map-1
  val l1: ActorRef = endpoint1.logs("L")
  val m1: ActorRef = endpoint1.logs("M")
  //#

  //#replication-endpoint-2
  val endpoint2 = new ReplicationEndpoint(id = "2", logNames = Set("L", "M"),
    logFactory = logId => LeveldbEventLog.props(logId),
    connections = Set(ReplicationConnection("127.0.0.1", 2552)))

  endpoint2.activate()
  //#

  import scala.concurrent.ExecutionContext.Implicits.global
  //#disaster-recovery-1
  import com.rbmhtechnology.eventuate.ReplicationEndpoint
  import scala.concurrent.Future
  import scala.util._

  val endpoint: ReplicationEndpoint = //...
  //#
  null

  //#disaster-recovery-1
  val recovery: Future[Unit] = endpoint.recover()

  recovery onComplete {
    case Success(_) => endpoint.activate()
    case Failure(e) => // retry recovery ...
  }
  //#

}

trait ReplicationFilterDoc {
  //#replication-filter-definition
  import com.rbmhtechnology.eventuate._

  case class AggregateIdFilter(aggregateId: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean =
      event.emitterAggregateId.contains(aggregateId)
  }
  //#

  //#replication-filter-application
  val filter1 = AggregateIdFilter("order-17")
  ReplicationConnection("127.0.0.1", 2553, Map("L" -> filter1))
  //#

  //#replication-filter-composition
  val filter2 = filter1 or AggregateIdFilter("order-19")
  ReplicationConnection("127.0.0.1", 2553, Map("M" -> filter2))
  //#
}