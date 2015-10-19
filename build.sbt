organization := "com.rbmhtechnology"

name := "eventuate"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.0"

scalacOptions in (Compile, doc) := List("-skip-packages", "akka")

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint"
)

connectInput in run := true

parallelExecution in Test := false

fork in Test := true

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies ++= Seq(
  "com.datastax.cassandra"           % "cassandra-driver-core"         % "2.1.5",
  "com.google.guava"                 % "guava"                         % "16.0",
  "com.google.protobuf"              % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"               %% "akka-remote"                   % akkaVersion,
  "com.typesafe.akka"               %% "akka-testkit"                  % akkaVersion  % "test,it",
  "com.typesafe.akka"               %% "akka-multi-node-testkit"       % akkaVersion  % "test",
  "commons-io"                       % "commons-io"                    % "2.4",
  "org.cassandraunit"                % "cassandra-unit"                % "2.0.2.2"    % "test,it" excludeAll(ExclusionRule(organization = "ch.qos.logback")),
  "org.functionaljava"               % "functionaljava"                % "4.2-beta-1" % "test",
  "org.functionaljava"               % "functionaljava-java8"          % "4.2-beta-1" % "test,it",
  "org.fusesource.leveldbjni"        % "leveldbjni-all"                % "1.8",
  "org.scalatest"                   %% "scalatest"                     % "2.1.4"      % "test,it",
  "org.scalaz"                      %% "scalaz-core"                   % "7.1.0"
)

// ----------------------------------------------------------------------
//  Documentation
// ----------------------------------------------------------------------

site.settings

site.sphinxSupport()

site.includeScaladoc()

ghpages.settings

git.remoteRepo := "git@github.com:RBMHTechnology/eventuate.git"

unmanagedSourceDirectories in Test += baseDirectory.value / "src" / "sphinx"/ "code"

// ----------------------------------------------------------------------
//  Publishing
// ----------------------------------------------------------------------

credentials += Credentials(
  "Artifactory Realm",
  "oss.jfrog.org",
  sys.env.getOrElse("OSS_JFROG_USER", ""),
  sys.env.getOrElse("OSS_JFROG_PASS", "")
)

publishTo := {
  val jfrog = "https://oss.jfrog.org/artifactory/"
  if (isSnapshot.value)
    Some("OJO Snapshots" at jfrog + "oss-snapshot-local")
  else
    Some("OJO Releases" at jfrog + "oss-release-local")
}

publishMavenStyle := true

// ----------------------------------------------------------------------
//  Integration and Multi-JVM testing
// ----------------------------------------------------------------------

import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

lazy val IntegrationTest = config("it") extend(Test)

evictionWarningOptions in update := EvictionWarningOptions.default
  .withWarnTransitiveEvictions(false)
  .withWarnDirectEvictions(false)
  .withWarnScalaVersionEviction(false)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(itSettings: _*)
  .configs(MultiJvm)
  .settings(multiJvmSettings: _*)

lazy val itSettings = Defaults.itSettings ++ Seq(
  compile in IntegrationTest <<= (compile in IntegrationTest) triggeredBy (compile in Test),
  test in IntegrationTest <<= (test in IntegrationTest) triggeredBy (test in Test),
  parallelExecution in IntegrationTest := false,
  fork in IntegrationTest := true
)

lazy val multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
  compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
  parallelExecution in Test := false,
  executeTests in Test <<=
    ((executeTests in Test), (executeTests in MultiJvm)) map {
      case ((testResults), (multiJvmResults)) =>
        val overall =
          if (testResults.overall.id < multiJvmResults.overall.id) multiJvmResults.overall
          else testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiJvmResults.events,
          testResults.summaries ++ multiJvmResults.summaries)
    }
)

// ----------------------------------------------------------------------
//  File headers
// ----------------------------------------------------------------------

val header =
  """|/*
     | * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
     | *
     | * Licensed under the Apache License, Version 2.0 (the "License");
     | * you may not use this file except in compliance with the License.
     | * You may obtain a copy of the License at
     | *
     | * http://www.apache.org/licenses/LICENSE-2.0
     | *
     | * Unless required by applicable law or agreed to in writing, software
     | * distributed under the License is distributed on an "AS IS" BASIS,
     | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     | * See the License for the specific language governing permissions and
     | * limitations under the License.
     | */
     |
     |""".stripMargin

headers := Map(
  "scala" -> (HeaderPattern.cStyleBlockComment, header),
  "java"  -> (HeaderPattern.cStyleBlockComment, header))

inConfig(Compile)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))
inConfig(Test)(compileInputs.in(compile) <<= compileInputs.in(compile).dependsOn(createHeaders.in(compile)))