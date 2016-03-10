import java.io.{FileWriter, BufferedWriter, File}
import java.text.SimpleDateFormat
import java.util.{Date, Timer}
import org.joda.time.{Period, Duration, DateTime}

import scala.io.Source
import scala.collection.JavaConversions._

import com.typesafe.config.ConfigFactory
import org.apache.spark.launcher.SparkLauncher

import launchers.{DummyLauncher, ReprocessLauncher, PTCrawlerLauncher}



object ProjectScheduler {

  def main(args: Array[String]) {

    val t: Timer = new Timer
    val period = 3600*24*1000
    //val confData = confReader("/opt/sds/mixedemotions/BRMProjectManager/conf/projects.conf")

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'")
    val reprocessStartDate : Date = sdf.parse("2016-01-01T16:44:00Z")
    val ptCrawlerStartDate : Date = sdf.parse("2016-01-01T21:40:00Z")


    //val reprocessLauncher = new ReprocessLauncher("/home/cnavarro/folders_sorted.txt")
    //t.schedule(reprocessLauncher, reprocessStartDate, period)
    val dummyLauncher = new DummyLauncher()
    t.schedule(dummyLauncher, reprocessStartDate, 10000)




    /*val paradigma_script_path = confData("paradigma_crawler_path").asInstanceOf[String]
    val pt_temp_conf_path = confData("paradigma_temp_conf_path").asInstanceOf[String]
    val projects_conf_path = confData("projects_conf_path").asInstanceOf[String]
    val ptCrawlerLauncher = new PTCrawlerLauncher(paradigma_script_path, projects_conf_path, pt_temp_conf_path)
    t.schedule(ptCrawlerLauncher, ptCrawlerStartDate, period)
    */




  }

  /*def reprocessFolders(): Unit ={
    val folderList = Source.fromFile("/home/cnavarro/folders_sorted.txt").getLines
    //val folderList = Source.fromFile("./src/main/resources/folders_sorted.txt").getLines
    val file = new File("scheduler.log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Starting\n")

    for(folder<-folderList){
      val start = DateTime.now()
      println(s"${start.toString} - Going to process folder ${folder}")
      bw.write(s"${start.toString} - Going to process folder ${folder}\n")
      try{
        sparkLaunch(folder)
        val spent = new Period(DateTime.now, start)

        println(s"Success! Took ${spent} with folder ${folder}")
        bw.write(s"Success! Took ${spent} with folder ${folder}\n")

      }catch {
        case e :Exception =>{
          println(s"Error executing folder: ${folder}")
          println(e)
          println(e.getStackTrace)
          bw.write(s"Error executing folder ${folder}\n")
          bw.write(e.toString +"\n")
          bw.write(e.getStackTrace.toString + "\n")
        }
      }
    }
    bw.close()


  }


  def sparkLaunch(folderPath: String):Unit = {

    val spark = new SparkLauncher()
      .setSparkHome("/opt/sds/spark/")
      .setAppResource("/home/cnavarro/BRMDemo_MarchMeeting-assembly-1.4.9.jar")
      .setMainClass("SparkOrchestrator")
      .setMaster("mesos://192.168.1.12:5050")
      .setConf(SparkLauncher.EXECUTOR_MEMORY, "9g")
      .setConf(SparkLauncher.EXECUTOR_CORES, "9")
      .addAppArgs(folderPath)
      .launch()
    spark.waitFor()
  }*/

  def schedule(): Unit ={
    val startDate = new Date()
    //val epoch : Long = 1454544 * 1000000
    //startDate.setTime(epoch)



    // Recovering conf data
    val confData = confReader("/opt/sds/mixedemotions/BRMProjectManager/conf/projects.conf")
    //val confData = confReader("/home/cnavarro/workspace/mixedemotions/me_extractors/BRMProjectManager/src/main/resources/application.conf")
    //val confData = confReader("application.conf")

    //val keywords = confData.get("keywords").asInstanceOf[Some[List[String]]].getOrElse(List[String](""))
    //val forbiddenkeywords = confData.get("forbidden_keywords").asInstanceOf[Some[List[String]]].getOrElse(List[String](""))
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
