import java.net.URL
import java.text.Normalizer

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Created by cnavarro on 7/10/15.
 */
class TopicExtractor(taxonomy: Map[String,List[String]])  extends Serializable {

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


}

object TopicExtractor{

  def readTaxonomy(url: URL): Map[String, List[String]] ={
    val json_string = Source.fromURL(url).mkString
    JSON.parseFull(json_string).get.asInstanceOf[Map[String,List[String]]]
  }

}