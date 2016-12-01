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

  //#replication-endpoint-3
  val endpoint3 = new ReplicationEndpoint(id = "3", logNames = Set("L", "M"),
    logFactory = logId => LeveldbEventLog.props(logId),
    connections = Set(ReplicationConnection("127.0.0.1", 2552)),
    applicationName = "example-application",
    applicationVersion = ApplicationVersion("1.2"))
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
    case Success(_) => // all events recovered, local writes are allowed
    case Failure(e) => // retry recovery ...
  }
  //#

  //#event-deletion-1
  // logically delete all events from log L1 with a sequence number <= 100
  // defer physical deletion until they are replicated to the given endpoints
  val logicallyDeleted: Future[Long] =
    endpoint.delete("L1", 100L, Set("remoteEndpointId1", "remoteEndpointId2"))

  logicallyDeleted onComplete {
    case Success(sequenceNr) => // events up to sequenceNr are logically deleted
    case Failure(e) => // deletion failed
  }
  //#

}

trait ReplicationFilterDoc {
  implicit val system: akka.actor.ActorSystem = null
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
  //#replication-filter-definition
  import com.rbmhtechnology.eventuate._

  case class AggregateIdFilter(aggregateId: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean =
      event.emitterAggregateId.contains(aggregateId)
  }
  //#

  //#local-filters
  val endpoint = new ReplicationEndpoint(id = "2", logNames = Set("L", "M"),
    logFactory = logId => LeveldbEventLog.props(logId),
    connections = Set(ReplicationConnection("127.0.0.1", 2553)),
    endpointFilters = EndpointFilters.sourceFilters(Map("L" -> filter1))
  )
  //#

  //#replication-filter-composition
  val filter1 = AggregateIdFilter("order-17")
  val filter2 = AggregateIdFilter("order-19")
  val composedFilter = filter1 or filter2
  //#


}
