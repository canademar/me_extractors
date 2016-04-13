package launchers

import java.io.File

import org.apache.spark.launcher.SparkLauncher
import launcherLogger.SparkLauncherLogger
import com.typesafe.config.{Config, ConfigFactory}



class MEOrchestratorLauncher(sparkHome: String, orchestratorJarPath: String, mainClass: String, sparkMaster: String,
                              executorMemory: String, executorCores: String, extraJars: List[String]) {



  /*def createSparkLauncher(): SparkLauncher ={
    new SparkLauncher()
      .setSparkHome("/opt/sds/spark/")
      .setAppResource("/home/cnavarro/BRMDemoReview-assembly-1.1.jar")
      .setMainClass("SparkOrchestrator")
      .setMaster("mesos://192.168.1.12:5050")
      .setConf(SparkLauncher.EXECUTOR_MEMORY, "9g")
      .setConf(SparkLauncher.EXECUTOR_CORES, "9")
      .addJar("/var/data/resources/nuig_entity_linking/lucene-core-4.10.4.jar")

  }*/
  def this(config: Config){
    this(config.getString("spark_home"), config.getString("orchestrator_jar_path"), config.getString("main_class"),
          config.getString("spark_master"), config.getString("executor_memory"), config.getString("executor_cores"),
          config.getStringList("extra_jars").toArray.toList.asInstanceOf[List[String]])


  }

  def this(configPath: String){
    this(MEOrchestratorLauncher.getConfig(configPath))
  }

  def createSparkLauncher(): SparkLauncher ={
    var launcher = new SparkLauncher()
      .setSparkHome(sparkHome)
      .setAppResource(orchestratorJarPath)
      .setMainClass(mainClass)
      .setMaster(sparkMaster)
      .setConf(SparkLauncher.EXECUTOR_MEMORY, executorMemory)
      .setConf(SparkLauncher.EXECUTOR_CORES, executorCores)
      for(extraJar: String<-extraJars) {
        launcher.addJar(extraJar)
      }
    launcher
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
    val sparkLauncher = createSparkLauncher()
    sparkLauncher.addAppArgs(folderPath)
    launchAndWait(sparkLauncher)

  }

}

object MEOrchestratorLauncher {
  def getConfig(configPath: String): Config ={
    val confFile = new File(configPath)
    val parsedConf = ConfigFactory.parseFile(confFile)
    ConfigFactory.load(parsedConf)
  }
}
