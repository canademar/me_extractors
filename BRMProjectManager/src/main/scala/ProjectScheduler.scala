import java.io.{FileWriter, BufferedWriter, File}
import java.text.SimpleDateFormat
import java.util.{Date, Timer}
import org.joda.time.{Period, Duration, DateTime}

import scala.io.Source
import scala.collection.JavaConversions._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.launcher.SparkLauncher

import ExecutionContext.Implicits.global

import launchers._



object ProjectScheduler {


  /*val configuration : Config = {
    val confFile = new File("application.conf")
    val parsedConf = ConfigFactory.parseFile(confFile)
    ConfigFactory.load(parsedConf)
  }*/

  def pathConfiguration(path: String) : Config = {
    val confFile = new File(path)
    val parsedConf = ConfigFactory.parseFile(confFile)
    ConfigFactory.load(parsedConf)
  }


  def main(args: Array[String]) {

    if(args.length==0){
      throw new Exception("Missing configuration path argument")
    }

    schedulerMain(args(0))

  }

  def schedulerMain(confPath : String) : Unit= {

    val t: Timer = new Timer
    //val period = 3600*24*1000
    val conf = pathConfiguration(confPath)

    val launcher : MEOrchestratorLauncher = new MEOrchestratorLauncher(conf.getString("orchestrator_conf_path"))


    val period = conf.getInt("hours_period")*3600*1000

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'")
    val reprocessStartDate : Date = sdf.parse("2016-01-01T" + conf.getString("reprocessing_start_time")+"Z")
    val processStartDate : Date = sdf.parse("2016-01-01T" + conf.getString("processing_start_time")+"Z")
    val ptCrawlerStartDate : Date = sdf.parse("2016-01-01T21:40:00Z")


    val reprocessLauncher = new ReprocessLauncher(launcher, conf.getString("folders_to_reprocess"), conf.getString("reprocess_log_path"))
    t.schedule(reprocessLauncher, reprocessStartDate, period)

    val projectIds : List[String] = conf.getString("project_ids").split(',').toList
    val dailyProcessLauncher = new DailyProcessLauncher(launcher, conf.getString("hdfs_data_folder"), projectIds, conf.getString("dailyprocessor_log_path"))
    t.schedule(dailyProcessLauncher, processStartDate, period)
    //val dummyLauncher = new DummyLauncher()
    //t.schedule(dummyLauncher, reprocessStartDate, 10000)





    /*val paradigma_script_path = confData("paradigma_crawler_path").asInstanceOf[String]
    val pt_temp_conf_path = confData("paradigma_temp_conf_path").asInstanceOf[String]
    val projects_conf_path = confData("projects_conf_path").asInstanceOf[String]
    val ptCrawlerLauncher = new PTCrawlerLauncher(paradigma_script_path, projects_conf_path, pt_temp_conf_path)
    t.schedule(ptCrawlerLauncher, ptCrawlerStartDate, period)
    */




  }





  def schedule(conf: Config): Unit ={
    val startDate = new Date()

    // Recovering conf data
    val confData = projectConfReader(conf.getString("projects_conf_path"))
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


  def projectConfReader(confFile: String) : Map[String, Any] = {

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

    output.toMap.asInstanceOf[Map[String, Any]]
  }



}
