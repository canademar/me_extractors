import java.io.File
import java.util.{Date, Timer}

import com.typesafe.config.ConfigFactory
import launchers.PTCrawlerLauncher

import scala.collection.JavaConversions._

object ProjectScheduler {

  def main(args: Array[String]) {
    val startDate = new Date()
    //val epoch : Long = 1454544 * 1000000
    //startDate.setTime(epoch)



    // Recovering conf data
    val confData = confReader("/opt/sds/mixedemotions/BRMProjectManager/conf/application2.conf")
    //val confData = confReader("/home/cnavarro/workspace/mixedemotions/me_extractors/BRMProjectManager/src/main/resources/application.conf")
    //val confData = confReader("application.conf")

    val keywords = confData.get("keywords").asInstanceOf[Some[List[String]]].getOrElse(List[String](""))
    val forbiddenkeywords = confData.get("forbidden_keywords").asInstanceOf[Some[List[String]]].getOrElse(List[String](""))
    val period = confData.get("period").asInstanceOf[Some[Long]].get

    // Launching the task (crawling + processing) every period milliseconds
    val t: Timer = new Timer
    //val mTask: TaskLauncher = new TaskLauncher(keywords)


    val paradigma_script_path = confData("paradigma_crawler_path").asInstanceOf[String]
    val pt_temp_conf_path = confData("paradigma_temp_conf_path").asInstanceOf[String]
    val projects_conf_path = confData("projects_conf_path").asInstanceOf[String]

    val ptCrawlerLauncher = new PTCrawlerLauncher(paradigma_script_path, projects_conf_path, pt_temp_conf_path)


    //t.scheduleAtFixedRate(mTask, 0, period)
    t.schedule(ptCrawlerLauncher, startDate, period)
  }


  def confReader(confFile: String) : scala.collection.mutable.Map[String, Any] = {

    val output = scala.collection.mutable.Map[String, Any]()

    //val conf = ConfigFactory.load(confFile)
    val parsedConf = ConfigFactory.parseFile(new File(confFile))
    val conf = ConfigFactory.load(parsedConf)
    // List of keywords
    val keywords = asScalaBuffer(conf.getStringList("keywords")).toList

    // val mylist = asScalaBuffer(keywords).toList

    // List of forbidden keywords
    val forbidden_keywords = asScalaBuffer(conf.getStringList("forbidden_keywords")).toList

    val period = conf.getLong("period")*60

    output += "keywords" -> keywords

    output += "forbidden_keywords" -> forbidden_keywords

    output += "period" -> period

    output += "paradigma_crawler_path" -> conf.getString("paradigma_crawler_path")

    output += "paradigma_temp_conf_path" -> conf.getString("paradigma_temp_conf_path")

    output += "projects_conf_path" -> conf.getString("projects_conf_path")

    output.asInstanceOf[scala.collection.mutable.Map[String, Any]]
  }

}
