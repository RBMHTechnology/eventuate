organization := "com.rbmhtechnology"

name := "eventuate"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.4"

val akkaVersion = "2.3.9"

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint"
)

connectInput in run := true

parallelExecution in Test := false

fork in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"              % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"               %% "akka-remote"                   % akkaVersion,
  "com.typesafe.akka"               %% "akka-testkit"                  % akkaVersion  % "test,it",
  "com.typesafe.akka"               %% "akka-multi-node-testkit"       % akkaVersion  % "test",
  "commons-io"                       % "commons-io"                    % "2.4",
  "org.functionaljava"               % "functionaljava"                % "4.2-beta-1" % "test",
  "org.functionaljava"               % "functionaljava-java8"          % "4.2-beta-1" % "test,it",
  "org.fusesource.leveldbjni"        % "leveldbjni-all"                % "1.7",
  "org.scalatest"                   %% "scalatest"                     % "2.1.4"      % "test,it",
  "org.scalaz"                      %% "scalaz-core"                   % "7.1.0"
)

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

