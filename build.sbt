organization := "com.rbmhtechnology"

name := "eventuate"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.4"

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint"
)

connectInput in run := true

fork in Test := true

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.google.protobuf"              % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"               %% "akka-remote"                   % "2.3.8",
  "com.typesafe.akka"               %% "akka-testkit"                  % "2.3.8"      % "test",
  "commons-io"                       % "commons-io"                    % "2.4",
  "org.functionaljava"               % "functionaljava"                % "4.2-beta-1" % "test",
  "org.functionaljava"               % "functionaljava-java8"          % "4.2-beta-1" % "test",
  "org.fusesource.leveldbjni"        % "leveldbjni-all"                % "1.7",
  "org.scalatest"                   %% "scalatest"                     % "2.1.4"      % "test",
  "org.scalaz"                      %% "scalaz-core"                   % "7.1.0"
)

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
