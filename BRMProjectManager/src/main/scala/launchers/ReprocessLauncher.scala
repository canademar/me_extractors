package launchers

import java.io.{BufferedWriter, File, FileWriter}
import java.util.TimerTask

import models.Project
import org.apache.spark.launcher.SparkLauncher
import org.joda.time.{Period, DateTime}

import scala.io.Source
import scala.sys.process.{ProcessLogger, _}


class ReprocessLauncher(orchestratorLauncher: MEOrchestratorLauncher, foldersToReprocessPath: String, reprocessLogPath: String) extends TimerTask {

  def run {
    println("Going to reprocess stuff")
    reprocessFolders(foldersToReprocessPath, reprocessLogPath)

  }

  def reprocessFolders(foldersToReprocessPath: String, reprocessLogPath: String): Unit = {
    val folderList = Source.fromFile(foldersToReprocessPath).getLines
    val file = new File(reprocessLogPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Starting\n")

    for (folder <- folderList) {
      val start = DateTime.now()
      println(s"${start.toString} - Going to process folder ${folder}")
      bw.write(s"${start.toString} - Going to process folder ${folder}\n")
      try {
        val launchedStatus : Int = orchestratorLauncher.launchOrchestratorAndWait(folder)

        val spent = new Period(start, DateTime.now)
        if(launchedStatus==0) {
          println(s"Success! Status ${launchedStatus}  Took ${spent.getHours}:${spent.getMinutes}:${spent.getSeconds} with folder ${folder}")
          bw.write(s"Success! Status ${launchedStatus} Took ${spent.getHours}:${spent.getMinutes}:${spent.getSeconds} with folder ${folder}\n")
        }else{
          println(s"Error ${launchedStatus}  after (${spent.getHours}:${spent.getMinutes}:${spent.getSeconds}) with folder ${folder}")
          bw.write(s"Error ${launchedStatus} after ${spent} (${spent.getHours}:${spent.getMinutes}:${spent.getSeconds}) with folder ${folder}\n")
        }
        println("----------------------------------")
        bw.write("---------------------------------\n")

      } catch {
        case e: Exception => {
          println(s"Error executing folder: ${folder}")
          println(e)
          println(e.getMessage)
          bw.write(s"Error executing folder ${folder}\n")
          bw.write(e.toString + "\n")
          bw.write(e.getMessage + "\n")
        }
      }
    }
    val end = DateTime.now()
    println(s"${end.toString} - Finished reprocessing\n")
    bw.write(s"${end.toString} - Finished reprocessing\n\n")
    bw.close()
  }


}

