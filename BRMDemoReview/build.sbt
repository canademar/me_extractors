name := "BRMDemoReview"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.lucene" % "lucene-core" % "4.10.0"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.0.0"

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-streams" % "1.7.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"

libraryDependencies += "org.json4s" % "json4s-jackson_2.10" % "3.2.11"

unmanagedJars in Compile += file("/home/cnavarro/workspace/mixedemotions/me_extractors/BRMPilot_y1/lib/entity-linking-1.1.jar")

unmanagedJars in Compile += file("/home/cnavarro/workspace/mixedemotions/me_extractors/BRMPilot_y1/lib/TweetSentiClassifier.jar")

assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
    