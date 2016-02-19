name := "Remote Connection"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
"com.typesafe.akka" %% "akka-actor" % "latest.integration",
"com.typesafe.akka" %% "akka-remote" % "latest.integration"
)

scalacOptions += "-deprecation"