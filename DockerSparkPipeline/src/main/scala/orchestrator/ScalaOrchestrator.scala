package orchestrator

import java.io.{FileWriter, BufferedWriter, File}
import java.text.Normalizer

import com.typesafe.config.{Config, ConfigFactory}
import legacy._
import org.slf4j.LoggerFactory
import services.{RESTService, DockerService, NotsFilter}
import utilities.{ElasticsearchPersistor, MarathonServiceDiscovery}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.parsing.json.JSON


object ScalaOrchestrator {
  val confFilePath = "/home/cnavarro/workspace/mixedemotions/me_extractors/DockerSparkPipeline/src/main/resources/dockerProject.conf"
  //val confFilePath = "/home/cnavarro/projectManager/conf/docker.conf"
  val logger = LoggerFactory.getLogger(ScalaOrchestrator.getClass)


  val configurationMap : Config = {
    println(s"ConfFile: ${confFilePath}")
    val confFile = new File(confFilePath)
    val parsedConf = ConfigFactory.parseFile(confFile)
    ConfigFactory.load(parsedConf)
  }

  def findMixEmModule(mod: String): List[String] => List[String] = {

    mod.trim match {
      case s if s.startsWith("docker") => dockerService(s)

      case s if s.startsWith("rest") => restService(s)

      case _ => throw new Exception("Module names should start by 'docker' or 'rest' in the 'modules' setting of the project configuration file")

    }

  }

  def dockerService(dockerName:String): List[String] => List[String] = {
    val serviceName = dockerName.replace("docker_","")
    val confFolder = configurationMap.getString("docker_conf_folder")
    val confPath = confFolder + serviceName + ".conf"
    println(s"Docker conf path: ${confPath}")
    val discoveryService = new MarathonServiceDiscovery(configurationMap.getString("mesos_dns.ip"), configurationMap.getInt("mesos_dns.port"))
    val service = DockerService.dockerServiceFromConfFile(confPath, discoveryService)
    service.executeServiceJSONList

  }


  def restService(restServiceName:String): List[String] => List[String] = {
    val serviceName = restServiceName.replace("rest_", "")
    val confFolder = configurationMap.getString("rest_conf_folder")
    val confPath = confFolder + serviceName + ".conf"
    println(s"Rest conf path: ${confPath}")

    val service = RESTService.restServiceFromConfFile(confPath)
    service.executeServiceJSONList
  }

  def saveToFile(input: List[String], outputPath:String): Unit ={
    println(s"Writing into ${outputPath} ")
    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(input.mkString("\n"))
    bw.close()
  }



  def main (args: Array[String]) {

    // Pipeline configuration
    val mods : List[String] = configurationMap.getStringList("modules").toList.reverse
    //mods.foreach(mod=>println("\n\n--------mod: " + mod + " -----------\n\n"))
    mods.foreach(mod=>logger.info("--------Loading mod: " + mod + " -----------\n"))
    //val mods = Array("persistor")


    // Loading data

    //println("\nLoading data  -------\n")
    logger.info("Starting  -------\n")

    val addData = Array("{\"text\": \"I hate western movies with John Wayne\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"La nueva de Star Wars está muy bien. Me encantó el robot pelota.\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"El jefe se va a Endesa.\", \"nots\": [\"hola\"], \"lang\": \"es\"}")


    val inputPath = args(0)
    println("inputPath")


    val initData = Source.fromFile(inputPath).getLines()
    //val data = initData.union(addData)
    val data : List[String] = initData.toList

    println("\nTotal number of raw data to process: " + data.length + "\n")

    // The NOT filter is initially applied tot he data
    //TODO: Not filter
    //val mydata = NotsFilter.filterText(data)
    //println("\nNumber of items after initial filtering: " + mydata.count() + "\n")



    // The name of the modules to be applied are stored in an array
    val funcArray = mods.map(findMixEmModule)

    // Getting the function that results from the composition of the selected modules/functions
    val dummyFunc: (List[String] => List[String]) = {x => x}
    val compFunc = funcArray.foldLeft(dummyFunc)(_.compose(_))
    println(s"Functions num: ${funcArray.length}")

    //funcArray.reduce(data)


    val resultJSON = compFunc(data)


    // Data are processed by the selected modules (composed function)
    //val resultJSON = compFunc(data)
    val numResultJSON = resultJSON.length

    println("\nNumber of items after processing (resultJSON): " + numResultJSON + "\n")

    val collected = resultJSON.toList
    println("Reactivate persistence")
    //persistWithoutSpark(collected)
    //collected.map(println(_))
    println(s"Num results: ${collected.length}")

    saveToFile(resultJSON,configurationMap.getString("outputFilePath"))


    /*val resultEn = resultJSON.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).filter(x=> x.getOrElse("lang","").asInstanceOf[String]=="en")
    val numResultEn = resultEn.count()

    println("\nNumber of items (in english) after processing (resultEn): " + numResultEn + "\n")

    if (numResultEn>10){
      val rEn = resultEn.take(10)
      rEn.foreach(println(_))
    }
    else{
      println("No data in english found")
    }
    */

  }




}
