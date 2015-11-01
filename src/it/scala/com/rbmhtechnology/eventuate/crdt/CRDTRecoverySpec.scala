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
      service1.add("x", 1)
      service1.add("x", 2)
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      service1.value("x").await should be(Set(1, 2))

      val service2 = service("b")
      // CRDT lazily recovered on read request
      service2.value("x").await should be(Set(1, 2))
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
    }
    "recover CRDT instances from snapshots" in {
      val service1 = service("a")
      service1.add("x", 1)
      service1.add("x", 2)
      service1.save("x").await
      service1.add("x", 3)
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      probe.expectMsg(Set(1, 2, 3))
      service1.value("x").await should be(Set(1, 2, 3))

      val service2 = service("a")
      // CRDT lazily recovered on read request
      service2.value("x").await should be(Set(1, 2, 3))
      // snapshot only exists in scope of service a
      probe.expectMsg(Set(1, 2))
      probe.expectMsg(Set(1, 2, 3))

      val service3 = service("b")
      // CRDT lazily recovered on read request
      service3.value("x").await should be(Set(1, 2, 3))
      // snapshot doesn't exist in scope of service b
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      probe.expectMsg(Set(1, 2, 3))
    }
  }
}

class CRDTRecoverySpecLeveldb extends CRDTRecoverySpec with EventLogLifecycleLeveldb
class CRDTRecoverySpecCassandra extends CRDTRecoverySpec with EventLogLifecycleCassandra