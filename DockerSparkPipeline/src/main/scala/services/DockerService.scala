package services

import java.io.File
import scala.util.parsing.json.JSON

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.slf4j.LoggerFactory

import utilities.{MarathonServiceDiscovery, RequestExecutor, ServiceConfParser}




class DockerService(serviceId: String, requestUrl: String, outputField:String, serviceDiscovery: MarathonServiceDiscovery,
                    method:String, body:String)
extends Serializable{



  def executeService(input: Map[String,Any]): Map[String, Any] ={
    val (ip, port) = serviceDiscovery.naiveServiceDiscover(serviceId)
    val url = ServiceConfParser.completeUrl(ip, port, requestUrl, input)
    println(s"Going to execute service:${url}")
    val response = RequestExecutor.executeGetRequest(url)
    //??? The response might be a single string or an array, not always a map
    val result = input + ((outputField,response))
    result

  }

  def executeService(input: Iterator[Map[String,Any]]) : Iterator[Map[String,Any]] = {
    for(entry<-input) yield {
      executeService(entry)
    }
  }

  def process(input: RDD[String]): RDD[String] = {
    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]()).asInstanceOf[Map[String,Any]])

    val processed = temp.mapPartitions(x => executeService(x))

    processed.mapPartitions(x => {

      implicit val formats = Serialization.formats(NoTypeHints)

      var a = List[String]()
      while(x.hasNext) {
        val r = x.next()
        a=a:+(write(r))
      }
      a.toIterator

    })
  }

}


object DockerService {

  def dockerServiceFromConfFile(confPath: String, serviceDiscovery: MarathonServiceDiscovery): DockerService ={
    val confFile = new File(confPath)
    val parsedConf = ConfigFactory.parseFile(confFile)
    val conf = ConfigFactory.load(parsedConf)
    new DockerService(conf.getString("serviceId"), conf.getString("requestUrl"), conf.getString("outputField"), serviceDiscovery, conf.getString("method"), conf.getString("body"))

  }



  
  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(DockerService.getClass)
    logger.info("Come ooooooooon")
    val inputs = Array("{\"text\": \"I hate western movies with John Wayne\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is really nasty. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is awesome, but maybe it is just for fans. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Hola ke ace?\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"La nueva de Star Wars está muy bien. Me encantó el robot pelota.\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"El jefe se va a Endesa.\", \"nots\": [\"hola\"], \"lang\": \"es\"}"
    )

    val discovery = new MarathonServiceDiscovery("localhost",8123)
    //val confPath = "/home/cnavarro/projectManager/conf/dockerServices/spanish_topic_service.conf"
    val confPath = "/home/cnavarro/workspace/mixedemotions/me_extractors/BRMDemoReview/src/main/resources/dockerServices/spanish_topic_service.conf"
    println(s"ConfPath:${confPath}")
    val dockerService = dockerServiceFromConfFile(confPath, discovery)


    for(input<-inputs){
      val inputMap = JSON.parseFull(input).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())
      val result = dockerService.executeService(inputMap)
      println(result)
      logger.info("Come ooooooooon")
    }

  }


}
