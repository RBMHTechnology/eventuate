package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import akka.testkit._

import com.rbmhtechnology.eventuate.log.EventLogSupport
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.concurrent._
import scala.concurrent.duration._

object CRDTServiceSpec {
  val config = ConfigFactory.parseString("log.leveldb.dir = target/test")
}

import CRDTServiceSpec._

class CRDTServiceSpec  extends TestKit(ActorSystem("test", config)) with WordSpecLike with Matchers with EventLogSupport {
  implicit class AwaitHelper[T](w: Awaitable[T]) {
    def await: T = Await.result(w, 3.seconds)
  }

  "A CRDTService" must {
    "manage multiple CRDTs identified by name" in {
      val service = new CounterService[Int]("a", log)
      service.update("a", 1).await should be(1)
      service.update("b", 2).await should be(2)
      service.value("a").await should be(1)
      service.value("b").await should be(2)
    }
  }

  "A CounterService" must {
    "return the default value of a Counter" in {
      val service = new CounterService[Int]("a", log)
      service.value("a").await should be(0)
    }
    "increment a Counter" in {
      val service = new CounterService[Int]("a", log)
      service.update("a", 3).await should be(3)
      service.update("a", 2).await should be(5)
      service.value("a").await should be(5)
    }
    "decrement a Counter" in {
      val service = new CounterService[Int]("a", log)
      service.update("a", -3).await should be(-3)
      service.update("a", -2).await should be(-5)
      service.value("a").await should be(-5)
    }
  }

  "An MVRegisterService" must {
    "return the default value of an MVRegister" in {
      val service = new MVRegisterService[Int]("a", log)
      service.value("a").await should be(Set())
    }
    "return the written value of an MVRegister" in {
      val service = new MVRegisterService[Int]("a", log)
      service.set("a", 1).await should be(Set(1))
      service.value("a").await should be(Set(1))
    }
  }

  "An ORSetService" must {
    "return the default value of an ORSet" in {
      val service = new ORSetService[Int]("a", log)
      service.value("a").await should be(Set())
    }
    "add an entry" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.value("a").await should be(Set(1))
    }
    "mask duplicates" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.add("a", 1).await should be(Set(1))
      service.value("a").await should be(Set(1))
    }
    "remove an entry" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.remove("a", 1).await should be(Set())
      service.value("a").await should be(Set())
    }
    "remove duplicates" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.add("a", 1).await should be(Set(1))
      service.remove("a", 1).await should be(Set())
      service.value("a").await should be(Set())
    }
  }
}
