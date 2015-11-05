package topic

import java.net.URL
import java.text.Normalizer

import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.parsing.json.JSON

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}


/**
 * Created by cnavarro on 8/10/15.
 */
class SparkTopicExtractor(taxonomy: Map[String,List[String]]) extends Serializable{

  def this(url: URL){
    this(TopicExtractor.readTaxonomy(url))
  }


  def sTcalculateTopics(text: String): scala.collection.mutable.Set[String] ={
    val cleanText = removeAccents(text)
    val lowerText = cleanText.toLowerCase
    var resultTopics =  scala.collection.mutable.Set[String]()
    for((key,topics) <- taxonomy){
      if(lowerText.contains(key)) {
        for (topic <- topics) {
          resultTopics += topic
        }
      }
    }
    resultTopics
  }


  def removeAccents(text: String): String ={
    val str = Normalizer.normalize(text,Normalizer.Form.NFD)
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    exp.replaceAllIn(str,"")
  }


  def extractTopics(input: RDD[String]): RDD[Map[String,Any]] = {
    input.map(line=>Map("line"->line, "topics"->sTcalculateTopics(line)))

  }

  def extractTopicsFromRDD(input: RDD[String]): RDD[String] = {

    val optionMaps = input.map(entry=>JSON.parseFull(entry).asInstanceOf[Some[Map[String,String]]])

    val maps = optionMaps.map(_.get)
    val results = maps.map(entry=>entry +(("topics", sTcalculateTopics(entry.getOrElse("text","")))))

    results.mapPartitions(entry=>SparkTopicExtractor.convertToJson(entry))
    //results.map(entry=>entry.mkString(","))

  }

}

object SparkTopicExtractor{
  def readTaxonomy(url: URL): Map[String, List[String]] ={
    val json_string = Source.fromURL(url).mkString
    JSON.parseFull(json_string).get.asInstanceOf[Map[String,List[String]]]
  }

  def convertToJson(lines: Iterator[Map[String, Any]]): Iterator[String] = {
    implicit val formats = Serialization.formats(NoTypeHints)
    for (line <- lines) yield {
      write(line)
    }
  }

}
