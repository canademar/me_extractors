name := "Spark NLP Project"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.clapper" %% "grizzled-slf4j" % "1.0.2")

val json4sNative = "org.json4s" %% "json4s-native" % "3.3.0"

val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.3.0"