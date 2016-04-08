package launchers

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, TimerTask}

import org.joda.time.{DateTime, Period}

import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source


class DailyProcessLauncher(baseFolder: String, projectIds: List[String]) extends TimerTask {


  def run {
    println("Going to reprocess stuff")
    val foldersToProcess : List[String] = getFoldersToProcess()
    processFolders(foldersToProcess)


  }

  def getYesterday() : String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val today = new DateTime()
    val yesterday = today.minusDays(1)
    sdf.format(yesterday.toDate)
  }

  def getFoldersToProcess(): List[String] ={
    val yesterdayStr : String = getYesterday()
    projectIds.flatMap(x => getSubfolders(s"${baseFolder}/${x}/${yesterdayStr}"))

  }

  def getSubfolders(folderPath: String) : List[String] = {
    List(folderPath +  "/twitter")

  }

  def processFolders(foldersToProcess: List[String]): Unit = {
    val file = new File("dailyProcessor.log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Starting\n")

    for (folder <- foldersToProcess) {
      val start = DateTime.now()
      println(s"${start.toString} - Going to process folder ${folder}")
      bw.write(s"${start.toString} - Going to process folder ${folder}\n")
      try {
        val launchedStatus : Int = MEOrchestratorLauncher.launchOrchestratorAndWait(folder)

        val spent = new Period(DateTime.now, start)
        if(launchedStatus==0) {
          println(s"Success! Status ${launchedStatus}  Took ${spent} (${spent.getHours}:${spent.getMinutes}:${spent.getSeconds}) with folder ${folder}")
          bw.write(s"Success! Status ${launchedStatus} Took ${spent} (${spent.getHours}:${spent.getMinutes}:${spent.getSeconds}) with folder ${folder}\n")
        }

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
    val end = DateTime.now()
    println(s"${end.toString} - Finished processing")
    bw.write(s"${end.toString} - Finished processing\n")
    bw.close()
  }


}

object DailyProcessLauncher {
  def main(args: Array[String]) {
    val ids = List("1")
    val baseFolder = "/user/stratio/"
    val daily : DailyProcessLauncher = new DailyProcessLauncher(baseFolder, ids)
    daily.run
    println(daily.getFoldersToProcess())

  }
}

