package launchers

import java.io.{BufferedWriter, File, FileWriter}
import java.util.TimerTask

import models.Project
import org.apache.spark.launcher.SparkLauncher
import org.joda.time.{Period, DateTime}

import scala.io.Source
import scala.sys.process.{ProcessLogger, _}


class ReprocessLauncher(foldersToReprocessPath: String) extends TimerTask {

  def run {
    println("Going to reprocess stuff")
    reprocessFolders(foldersToReprocessPath)

  }

  def reprocessFolders(foldersToReprocessPath: String): Unit = {
    val folderList = Source.fromFile(foldersToReprocessPath).getLines
    val file = new File("scheduler.log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Starting\n")

    for (folder <- folderList) {
      val start = DateTime.now()
      println(s"${start.toString} - Going to process folder ${folder}")
      bw.write(s"${start.toString} - Going to process folder ${folder}\n")
      try {
        val sparkLaunched = MEOrchestratorLauncher.launchOrchestratorAndWait(folder)

        val spent = new Period(DateTime.now, start)

        println(s"Success! Took ${spent} with folder ${folder}")
        bw.write(s"Success! Took ${spent} with folder ${folder}\n")

      } catch {
        case e: Exception => {
          println(s"Error executing folder: ${folder}")
          println(e)
          println(e.getStackTrace)
          bw.write(s"Error executing folder ${folder}\n")
          bw.write(e.toString + "\n")
          bw.write(e.getStackTrace.toString + "\n")
        }
      }
    }
    bw.close()
  }




  /*def sparkLauncher():Unit = {

    val spark = new SparkLauncher()
      .setSparkHome("/opt/sds/spark/")
      .setAppResource("/home/jvmarcos/DummySparkApp-assembly-1.0.jar")
      .setMainClass("DummySparkApp")
      .setMaster("mesos://192.168.1.12:5050")
      .launch()
    spark.waitFor()
  }*/


}

