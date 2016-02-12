/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.example.dbreplica

import akka.actor._

import com.rbmhtechnology.example.dbreplica.cdc._
import com.rbmhtechnology.example.dbreplica.domain._
import com.rbmhtechnology.example.dbreplica.event._
import com.rbmhtechnology.example.dbreplica.repository._
import com.rbmhtechnology.example.dbreplica.service._
import com.rbmhtechnology.eventuate._
import com.typesafe.config._

import org.springframework.context.annotation._
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource._
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement

import scala.collection.immutable.Seq
import scala.io.Source

@Configuration
class LocationA extends DBReplica {
  override def config: Config =
    ConfigFactory.load("dbreplica/location-A.conf")
}

@Configuration
class LocationB extends DBReplica {
  override def config: Config =
    ConfigFactory.load("dbreplica/location-B.conf")
}

@Configuration
class LocationC extends DBReplica {
  override def config: Config =
    ConfigFactory.load("dbreplica/location-C.conf")
}

@EnableAspectJAutoProxy
@EnableTransactionManagement
@ComponentScan(Array("com.rbmhtechnology.example.dbreplica"))
abstract class DBReplica {
  @Bean
  def config: Config

  @Bean
  def settings: AssetCdcSettings =
    new AssetCdcSettings(config)

  @Bean
  def storageBackend: StorageBackend =
    new StorageBackend(config)

  @Bean
  def txManager: PlatformTransactionManager =
    new DataSourceTransactionManager(storageBackend.dataSource)

  @Bean
  def jdbcTemplate: JdbcTemplate =
    new JdbcTemplate(storageBackend.dataSource)

  @Bean(destroyMethod = "terminate")
  def system: ActorSystem =
    ActorSystem(ReplicationConnection.DefaultRemoteSystemName, config)
}

class DBReplicaCLI(service: AssetCdcOutbound, finder: AssetFinder) extends Actor {
  val lines = Source.stdin.getLines

  def receive = {
    case line: String => line.split(' ').toList match {
      case "create" :: id :: s :: c :: Nil =>
        service.handle(AssetCreated(id, s, c)); prompt()
      case "subject" :: id :: s :: Nil =>
        service.handle(AssetSubjectUpdated(id, s)); prompt()
      case "content" :: id :: c :: Nil =>
        service.handle(AssetContentUpdated(id, c)); prompt()
      case "list" :: Nil =>
        finder.findAll.foreach(println); prompt()
      case Nil       => prompt()
      case "" :: Nil => prompt()
      case na :: nas => println(s"unknown command: ${na}"); prompt()
    }
  }

  def prompt(): Unit = {
    if (lines.hasNext) lines.next() match {
      case "exit" => context.system.terminate()
      case line   => self ! line
    }
  }

  override def preStart(): Unit =
    prompt()
}

class DBReplicaListener extends AssetListener {
  override def assetCreated(asset: Asset): Unit =
    println(s"created: $asset")

  override def assetUpdated(asset: Asset): Unit =
    println(s"updated: $asset")

  override def assetSelected(asset: Asset, conflicts: Seq[Asset]): Unit = {
    conflicts.foreach { asset =>
      println(s"conflict: $asset")
    }
    println(s"selected: $asset")
  }
}

object DBReplica extends App {
  val location = args(0)
  val context = new AnnotationConfigApplicationContext(Class.forName(s"com.rbmhtechnology.example.dbreplica.Location${location}"))
  val listeners = context.getBean(classOf[AssetListeners])
  val service = context.getBean(classOf[AssetCdcOutbound])
  val finder = context.getBean(classOf[AssetFinder])
  val system = context.getBean(classOf[ActorSystem])

  listeners.addListener(new DBReplicaListener)
  system.actorOf(Props(new DBReplicaCLI(service, finder)).withDispatcher("eventuate.cli-dispatcher"))
}
