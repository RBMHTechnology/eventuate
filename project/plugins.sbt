resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.3.8")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.8.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.3")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.0.0")

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.4.0")

libraryDependencies += "com.github.os72" % "protoc-jar" % "2.x.5"
