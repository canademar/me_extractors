package launchers

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, TimerTask}

import org.joda.time.{DateTime, Period}

import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source


class DailyProcessLauncher(orchestratorLauncher: MEOrchestratorLauncher, baseFolder: String, projectIds: List[String],
                            dailyProcessorLogPath: String) extends TimerTask {


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
    val file = new File(dailyProcessorLogPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Starting\n")

    for (folder <- foldersToProcess) {
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
          println(s"-->Error!!! Status ${launchedStatus}  Took ${spent.getHours}:${spent.getMinutes}:${spent.getSeconds} with folder ${folder}")
          bw.write(s"-->Error!!! Status ${launchedStatus} Took ${spent.getHours}:${spent.getMinutes}:${spent.getSeconds} with folder ${folder}\n")
        }
        println("----------\n")
        bw.write("----------\n\n")

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
    println(s"${end.toString} - Finished processing")
    bw.write(s"${end.toString} - Finished processing\n")
    bw.close()
  }


}


