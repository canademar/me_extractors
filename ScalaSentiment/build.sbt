

name := "ScalaSentiment"

version := "1.0"

//javaHome := Some(file("/home/cnavarro/jdk1.8.0_65"))

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
