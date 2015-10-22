package topic

import java.net.URL
import java.text.Normalizer

import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Created by cnavarro on 8/10/15.
 */
class SparkTopicExtractor(taxonomy: Map[String,List[String]]) extends Serializable{
  def this(url: URL){
    this(TopicExtractor.readTaxonomy(url))
  }


  def calculateTopics(text: String): scala.collection.mutable.Set[String] ={
    val cleanText = removeAccents(text)
    val lowerText = cleanText.toLowerCase
    var resultTopics =  scala.collection.mutable.Set[String]()
    for((key,topics) <- taxonomy){
      if(lowerText.contains(key)) {
        println(topics)
        println(topics.getClass)
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
    input.map(line=>Map("line"->line, "topics"->calculateTopics(line)))

  }

  def extractTopicsFromRDD(input: RDD[Map[String,Any]]): RDD[Map[String,Any]] = {
    input.map(line=>Map("text"->line.getOrElse("text",""), "topics"->calculateTopics(line.getOrElse("text","").asInstanceOf[String])))

  }

}

object SparkTopicExtractor{
  def readTaxonomy(url: URL): Map[String, List[String]] ={
    val json_string = Source.fromURL(url).mkString
    JSON.parseFull(json_string).get.asInstanceOf[Map[String,List[String]]]
  }
}
