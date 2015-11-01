package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import akka.testkit._

import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.utilities._

import org.scalatest._

abstract class CRDTRecoverySpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterEach {
  var probe: TestProbe = _

  def log: ActorRef

  def service(serviceId: String) = new ORSetService[Int](serviceId, log) {
    override def onChange(crdt: ORSet[Int], operation: Any): Unit = probe.ref ! crdt.value
  }

  override protected def beforeEach(): Unit =
    probe = TestProbe()

  "A CRDTService" must {
    "recover CRDT instances" in {
      val service1 = service("a")
      service1.add("a", 1)
      service1.add("a", 2)
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      service1.value("a").await should be(Set(1, 2))

      val service2 = service("b")
      service2.value("a").await should be(Set(1, 2))
      // CRDT lazily recovered on read request
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
    }
  }
}

class CRDTRecoverySpecLeveldb extends CRDTRecoverySpec with EventLogLifecycleLeveldb
class CRDTRecoverySpecCassandra extends CRDTRecoverySpec with EventLogLifecycleCassandra