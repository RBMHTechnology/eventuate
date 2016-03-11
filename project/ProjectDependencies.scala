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

import sbt._

object ProjectDependencyVersions {
  val AkkaVersion = "2.4.2"
  val ProtobufVersion = "2.5.0"
}

object ProjectDependencies {
  import ProjectDependencyVersions._

  val AkkaRemote = "com.typesafe.akka" %% "akka-remote" % AkkaVersion
  val AkkaTestkit = "com.typesafe.akka" %% "akka-testkit" % AkkaVersion
  val AkkaTestkitMultiNode = "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion
  val CassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5"
  val CassandraUnit = "org.cassandraunit" % "cassandra-unit" % "2.0.2.2"
  val CommonsIo =  "commons-io" % "commons-io" % "2.4"
  val Leveldb = "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
  val Java8Compat = "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.7.0"
  val Javaslang = "com.javaslang" % "javaslang" % "2.0.0-RC3"
  val JunitInterface = "com.novocode" % "junit-interface" % "0.11"
  val Protobuf = "com.google.protobuf" % "protobuf-java" % ProtobufVersion
  val Scalatest = "org.scalatest" %% "scalatest" % "2.1.4"
  val Scalaz = "org.scalaz" %% "scalaz-core" % "7.1.0"
}

