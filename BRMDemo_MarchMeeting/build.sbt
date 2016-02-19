name := "BRMDemo_MarchMeeting"

version := "1.4.4"

scalaVersion := "2.10.4"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.0.0"

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-streams" % "1.7.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" //% "provided"

libraryDependencies += "org.json4s" % "json4s-jackson_2.10" % "3.2.11"

assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

    