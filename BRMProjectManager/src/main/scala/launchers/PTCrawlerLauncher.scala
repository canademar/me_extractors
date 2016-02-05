package launchers

import java.io.{File,FileWriter, BufferedWriter}
import java.util.TimerTask

import org.apache.spark.launcher.SparkLauncher
import org.joda.time.DateTime

import models.Project
import scala.sys.process
import scala.sys.process._
import scala.sys.process.ProcessLogger


class PTCrawlerLauncher(crawlerPath:String, projectsPath: String, tempProjectJsonPath: String) extends TimerTask {

  def run {
    val source = scala.io.Source.fromFile(projectsPath)


    try {
      val lines = source.getLines().mkString("\n")
      val projects = Project.projectsFromJson(lines)

      val today = DateTime.now()
      var newProjects = List[Project]()
      // 1. The crawler is launched
      for (project: Project <- projects) {
        println("Today : " + today)
        println("Project day : " + project.start.toDate)

        if(project.start.dayOfYear().get() <= today.dayOfYear().get() & project.start.plusDays(1).dayOfYear().get() > today.dayOfYear().get()){
          newProjects = project :: newProjects
        }

      }
      launchNewProjects(newProjects)
      launchAllProjects

      // 2. The Spark process is launched

      // println("Launching the Spark app")
    }finally{
      source.close()
    }


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

  def launchNewProjects(projects: List[Project]): Unit = {
    println("Launching new project")



    writeToTempJson(projects, tempProjectJsonPath)

    val commandStr = "python " + crawlerPath + " new_projects " + tempProjectJsonPath
    println(commandStr)

    val stdout = new StringBuilder
    val stderr = new StringBuilder

    //val status = commandStr.lines_!(ProcessLogger(stdout append _, stderr append _))
    val status = commandStr.lines_!(ProcessLogger(stdout append _, stderr append _))
    //println(status)
    //println("stdout: " + stdout)
    //println("stderr: " + stderr)
    //print(stdout)
    println("Continuing")

  }

  def launchAllProjects(): Unit = {
    println("Launching all projects")

  }

  def writeToTempJson(projects: List[Project], filePath: String): Unit = {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(Project.toJson(projects))
    bw.close()
  }

}

object PTCrawlerLauncher {
  //def main(args: Array[String]) {
  //  println("pwd".!!)
  //  val pt_crawler = new PTCrawlerLauncher("/home/cnavarro/workspace/mixedemotions/me_extractors/crawlers/paradigma_python_harvester/paradigma_harvester.py",
  //    "/home/cnavarro/workspace/mixedemotions/me_extractors/BRMProjectManager/src/main/resources/projects.json",
  //    "/home/cnavarro/workspace/mixedemotions/me_extractors/crawlers/paradigma_python_harvester/temp.json")
  //  pt_crawler.run
  //}
}