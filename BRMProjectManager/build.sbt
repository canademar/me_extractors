name := "BRMProjectManager"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}


    