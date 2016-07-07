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
  val AkkaVersion = "2.4.4"
  val CassandraVersion = "3.4"
  val Log4jVersion = "2.5"
  val ProtobufVersion = "2.5.0"
  val SparkVersion = "1.6.1"
  val VertxVersion = "3.0.0"
  val ExampleVertxVersion = "3.3.2"
}

object ProjectDependencies {
  import ProjectDependencyVersions._

  val CassandraDriver =      "com.datastax.cassandra"     % "cassandra-driver-core"     % "3.0.2"
  val CassandraConnector =   "com.datastax.spark"        %% "spark-cassandra-connector" % "1.6.0-M2"
  val Javaslang =            "com.javaslang"              % "javaslang"                 % "2.0.0-RC3"
  val Protobuf =             "com.google.protobuf"        % "protobuf-java"             % ProtobufVersion
  val JunitInterface =       "com.novocode"               % "junit-interface"           % "0.11"
  val AkkaRemote =           "com.typesafe.akka"         %% "akka-remote"               % AkkaVersion
  val AkkaTestkit =          "com.typesafe.akka"         %% "akka-testkit"              % AkkaVersion
  val AkkaTestkitMultiNode = "com.typesafe.akka"         %% "akka-multi-node-testkit"   % AkkaVersion
  val CommonsIo =            "commons-io"                 % "commons-io"                % "2.4"
  val CassandraClientUtil =  "org.apache.cassandra"       % "cassandra-clientutil"      % CassandraVersion
  val Log4jApi =             "org.apache.logging.log4j"   % "log4j-api"                 % Log4jVersion
  val Log4jCore =            "org.apache.logging.log4j"   % "log4j-core"                % Log4jVersion
  val Log4jSlf4j =           "org.apache.logging.log4j"   % "log4j-slf4j-impl"          % Log4jVersion
  val SparkCore =            "org.apache.spark"          %% "spark-core"                % SparkVersion
  val SparkSql =             "org.apache.spark"          %% "spark-sql"                 % SparkVersion
  val SparkStreaming =       "org.apache.spark"          %% "spark-streaming"           % SparkVersion
  val CassandraUnit =        "org.cassandraunit"          % "cassandra-unit"            % "3.0.0.1"
  val Leveldb =              "org.fusesource.leveldbjni"  % "leveldbjni-all"            % "1.8"
  val Sigar =                "org.fusesource"             % "sigar"                     % "1.6.4"
  val Java8Compat =          "org.scala-lang.modules"     % "scala-java8-compat_2.11"   % "0.7.0"
  val Scalatest =            "org.scalatest"             %% "scalatest"                 % "2.1.4"
  val Scalaz =               "org.scalaz"                %% "scalaz-core"               % "7.1.0"
  val VertxCore =            "io.vertx"                   % "vertx-core"                % VertxVersion
  val VertxRxJava =          "io.vertx"                   % "vertx-rx-java"             % VertxVersion
  val ExampleVertxCore =     "io.vertx"                   % "vertx-core"                % ExampleVertxVersion
  val ExampleVertxRxJava =   "io.vertx"                   % "vertx-rx-java"             % ExampleVertxVersion
}

