import java.util.TimerTask

import org.apache.spark.launcher.SparkLauncher

class TaskLauncher(keywords: List[String]) extends TimerTask {

  def run {

    // 1. The crawler is launched
    for(keyword <- keywords) {
      println("Launching crawler - Searching: " + keyword)
    }

    // 2. The Spark process is launched
    sparkLauncher()
    // println("Launching the Spark app")


  }

  def sparkLauncher():Unit = {

    val spark = new SparkLauncher()
      .setSparkHome("/opt/sds/spark/")
      .setAppResource("/home/jvmarcos/DummySparkApp-assembly-1.0.jar")
      .setMainClass("DummySparkApp")
      .setMaster("mesos://192.168.1.12:5050")
      .launch()
    spark.waitFor()
  }

}