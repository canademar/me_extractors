package launchers

import org.apache.spark.launcher.SparkLauncher
import launcherLogger.SparkLauncherLogger



object MEOrchestratorLauncher {



  val sparkLauncher: SparkLauncher ={
    new SparkLauncher()
      .setSparkHome("/opt/sds/spark/")
      .setAppResource("/home/cnavarro/BRMDemoReview-assembly-1.1.jar")
      .setMainClass("SparkOrchestrator")
      .setMaster("mesos://192.168.1.12:5050")
      .setConf(SparkLauncher.EXECUTOR_MEMORY, "9g")
      .setConf(SparkLauncher.EXECUTOR_CORES, "9")
      .addJar("/var/data/resources/nuig_entity_linking/lucene-core-4.10.4.jar")

  }

  def launchAndWait(sparkLauncher: SparkLauncher): Int ={
    val process = sparkLauncher.launch()

    val inputStreamReaderRunnable : SparkLauncherLogger = new SparkLauncherLogger(process.getInputStream(), "sparkLauncherStdOut")
    val inputThread: Thread = new Thread(inputStreamReaderRunnable, "LogStreamReader input")
    inputThread.start()

    val errorStreamReaderRunnable : SparkLauncherLogger = new SparkLauncherLogger(process.getErrorStream(), "sparkLauncherError")
    val errorThread : Thread = new Thread(errorStreamReaderRunnable, "LogStreamReader error")
    errorThread.start()

    val status = process.waitFor()
    if(status!=0){
      throw new Exception("Error processing with spark launcher")
    }
    status
  }

  def launchOrchestratorAndWait(folderPath: String): Int  ={
    println(s"Launching orchestrator and waiting: ${folderPath}")
    //val sparkLauncher = sparkLauncher()
    sparkLauncher.addAppArgs(folderPath)
    launchAndWait(sparkLauncher)

  }

}
