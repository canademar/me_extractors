import java.util.Timer

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

object ProjectScheduler {

  def main(args: Array[String]) {


    // Recovering conf data
    val confData = confReader("application.conf")

    val keywords = confData.get("keywords").asInstanceOf[Some[List[String]]].getOrElse(List[String](""))
    val forbiddenkeywords = confData.get("forbidden_keywords").asInstanceOf[Some[List[String]]].getOrElse(List[String](""))
    val period = confData.get("period").asInstanceOf[Some[Long]].get

    // Launching the task (crawling + processing) every period milliseconds
    val t: Timer = new Timer
    val mTask: TaskLauncher = new TaskLauncher(keywords)
    t.scheduleAtFixedRate(mTask, 0, period)
  }


  def confReader(confFile: String) : scala.collection.mutable.Map[String, Any] = {

    val output = scala.collection.mutable.Map[String, Any]()

    val conf = ConfigFactory.load("application.conf")
    // List of keywords
    val keywords = asScalaBuffer(conf.getStringList("keywords")).toList

    // val mylist = asScalaBuffer(keywords).toList

    // List of forbidden keywords
    val forbidden_keywords = asScalaBuffer(conf.getStringList("forbidden_keywords")).toList

    val period = conf.getLong("period")

    output += "keywords" -> keywords

    output += "forbidden_keywords" -> forbidden_keywords

    output += "period" -> period

    output.asInstanceOf[scala.collection.mutable.Map[String, Any]]
  }

}
