import java.io.File
import java.net.URLEncoder
import scala.collection.JavaConversions._

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON


class DockerService(serviceId: String, inputMap: Map[String, String], outputField:String, serviceDiscovery: MarathonServiceDiscovery){



  def executeService(input: Map[String,Any]): Map[String, Any] ={
    val url = composeQuery(input)
    println(s"Going to execute service:${url}")
    val response = NetworkAnalysisService.executeGetRequest(url)
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

  private
  def composeQuery(input: Map[String,Any]): String = {

    println("Compose Query")
    val params = inputMap.map{case (paramKey, inputKey) => (paramKey, input(inputKey) )}
    val baseUrl = serviceDiscovery.naiveServiceDiscoverURL(serviceId)
    val paramsString = params.map{ case (a,b) => s"${a}=${URLEncoder.encode(b.toString,"UTF-8")}"}.mkString("&")
    val url = baseUrl + "?"+ paramsString
    println("url found")
    url
  }

}


object DockerService {

  def dockerServiceFromConfFile(confPath: String, serviceDiscovery: MarathonServiceDiscovery): DockerService ={
    val confFile = new File(confPath)
    val parsedConf = ConfigFactory.parseFile(confFile)
    val conf = ConfigFactory.load(parsedConf)
    val fieldsList : List[String] = conf.getStringList("inputMap").toList
    val inputMap = fieldsList.map(_.split("=>")).map(pair=>Map((pair(0),pair(1))))
    new DockerService(conf.getString("serviceId"), null, conf.getString("outputField"), serviceDiscovery)

  }

  def main(args: Array[String]) {
    val inputs = Array("{\"text\": \"I hate western movies with John Wayne\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is really nasty. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is awesome, but maybe it is just for fans. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Hola ke ace?\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"La nueva de Star Wars está muy bien. Me encantó el robot pelota.\", \"nots\": [\"hola\"], \"lang\": \"es\"}")

    val discovery = new MarathonServiceDiscovery("localhost",8123)
    val dockerService = new DockerService("topic-container", Map(("text","text")), "topics", discovery )


    for(input<-inputs){
      val inputMap = JSON.parseFull(input).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())
      val result = dockerService.executeService(inputMap)
      println(result)
    }

  }


}
