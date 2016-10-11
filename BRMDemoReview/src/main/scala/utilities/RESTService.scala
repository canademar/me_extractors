package utilities

import java.net.URLEncoder

import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON

/**
 * Created by cnavarro on 4/07/16.
 */
class RESTService(requestUrl: String, method: String, body: String, ip: String, port:Int, outputField:String)
  extends Serializable{



  def executeService(input: Map[String,Any]): Map[String, Any] ={
    val url = composeQuery(input)
    println(s"Going to execute service:${url}")
    val response = RequestExecutor.executeRequest(method, url, body)
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
    println(s"input:${input}")
    val composedUrl = ServiceConfParser.completeUrl(ip,port,requestUrl)
    println("url found")
    println(s"url: '${composedUrl}")
    composedUrl
  }



}
