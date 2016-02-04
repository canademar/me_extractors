import com.typesafe.config.ConfigFactory
import org.apache.spark.launcher.SparkLauncher

object MEProjectManager {

  def oldMain(args: Array[String]) {

    val confData = confReader("application.conf")

    val keywords = confData._1.toArray.asInstanceOf[Array[String]]
    val forbiddenkeywords = confData._2.toArray.asInstanceOf[Array[String]]

    for(keyword <- keywords) {
      println("Launching crawler - Searching: " + keyword)
    }

    sparkLauncher()

  }

  def confReader(confFile: String) : (java.util.ArrayList[String], java.util.ArrayList[String]) = {

    val conf = ConfigFactory.load("application.conf")
    // List of keywords
    val keywords = conf.getStringList("keywords")

    // List of forbidden keywords
    val forbidden_keywords = conf.getStringList("forbidden_keywords")

    (keywords.asInstanceOf[java.util.ArrayList[String]], forbidden_keywords.asInstanceOf[java.util.ArrayList[String]])

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
