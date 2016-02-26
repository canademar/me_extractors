name := "BRMProjectManager"

version := "1.1"

scalaVersion := "2.10.4"

mainClass := Some("ProjectScheduler")

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"



libraryDependencies += "joda-time" % "joda-time" % "2.9.2"

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

libraryDependencies += "org.json4s" % "json4s-jackson_2.10" % "3.2.11"

assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}


    