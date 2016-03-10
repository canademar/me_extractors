package launchers

import org.apache.spark.launcher.SparkLauncher

object MEOrchestratorLauncher {



  def getSparkLauncher(): SparkLauncher ={
    new SparkLauncher()
      .setSparkHome("/opt/sds/spark/")
      .setAppResource("/home/cnavarro/BRMDemo_MarchMeeting-assembly-1.4.9.jar")
      .setMainClass("SparkOrchestrator")
      .setMaster("mesos://192.168.1.12:5050")
      .setConf(SparkLauncher.EXECUTOR_MEMORY, "9g")
      .setConf(SparkLauncher.EXECUTOR_CORES, "9")
  }

  def launchAndWait(sparkLauncher: SparkLauncher): Unit ={
    val spark = sparkLauncher.launch()
    spark.waitFor()
  }

  def launchOrchestratorAndWait(folderPath: String): Int  ={
    val sparkLauncher = getSparkLauncher()
    sparkLauncher.setAppResource(folderPath)
    val spark = sparkLauncher.launch()
    spark.waitFor()

  }

}
