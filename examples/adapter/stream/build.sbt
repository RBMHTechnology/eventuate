organization := "com.micronautics"

name := "eventuate-stream-adapter-example"

version := "0.2.0"

scalaVersion := "2.12.1"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-target:jvm-1.8",
  "-unchecked",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xlint"
)

scalacOptions in (Compile, doc) ++= baseDirectory.map {
  (bd: File) => Seq[String](
    "-sourcepath", bd.getAbsolutePath,
    "-doc-source-url", "https://github.com/mslinn/eventuate/tree/master€{FILE_PATH}.scala"
  )
}.value

javacOptions ++= Seq(
  "-Xlint:deprecation",
  "-Xlint:unchecked",
  "-source", "1.8",
  "-target", "1.8",
  "-g:vars"
)

resolvers += "Eventuate Releases" at "https://dl.bintray.com/rbmhtechnology/maven"

val evVer = "0.8.1"

libraryDependencies ++= Seq(
  "com.rbmhtechnology" %% "eventuate-adapter-stream" % evVer withSources(),
  //  "com.rbmhtechnology" %% "eventuate-adapter-vertx"  % evVer withSources(),
  //  "com.rbmhtechnology" %% "eventuate-adapter-spark"  % evVer withSources(),
  "com.rbmhtechnology" %% "eventuate-core"           % evVer withSources(),
  //  "com.rbmhtechnology" %% "eventuate-crdt"           % evVer withSources(),
  //  "com.rbmhtechnology" %% "eventuate-log-cassandra"  % evVer withSources(),
  "com.rbmhtechnology" %% "eventuate-log-leveldb"    % evVer withSources(),
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" withSources(),
  //
  "org.scalatest"     %% "scalatest"   % "3.0.1" % Test withSources(),
  "junit"             %  "junit"       % "4.12"  % Test
)

parallelExecution in Test := false
fork in Test := true
fork in Runtime := true

logLevel := Level.Warn

// Only show warnings and errors on the screen for compilations.
// This applies to both test:compile and compile and is Info by default
logLevel in compile := Level.Warn

// Level.INFO is needed to see detailed output when running tests
logLevel in test := Level.Info

// define the statements initially evaluated when entering 'console', 'console-quick', but not 'console-project'
initialCommands in console := """import akka.actor._
                                |import com.rbmhtechnology.eventuate.EventsourcedActor
                                |import scala.util._
                                |""".stripMargin

cancelable := true
