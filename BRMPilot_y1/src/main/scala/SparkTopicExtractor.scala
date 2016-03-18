import java.text.Normalizer

import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.util.parsing.json.JSON


/**
 * Created by cnavarro on 8/10/15.
 */
class SparkTopicExtractor(taxonomy: Map[String,List[String]]) extends Serializable{

  def calculateTopics(text: String): scala.collection.mutable.Set[String] ={
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

  def extractTopicsFromRDD(input: RDD[String]): RDD[String] = {

    println("!22222222222222222222 Extract topics from RDD")

    val orig = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]()))
      .map(x => collection.mutable.Map(x.toSeq: _*)).map(x => extractTopicsFromMap(x))

    val result = orig.mapPartitions(x=> {

      implicit val formats = Serialization.formats(NoTypeHints)

      var a = List[String]()

      while(x.hasNext) {
        val r = x.next()
        a.::=(write(r))
      }
      a.toIterator
    })

    result

  }

  def extractTopicsFromMap(input: scala.collection.mutable.Map[String,Any]): scala.collection.mutable
    .Map[String,Any] = {

    input += ("topics"->calculateTopics(input.getOrElse("text","").asInstanceOf[String]))

  }

}


