import java.net.URL
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


  def this(url: URL){
    this(TopicExtractor.readTaxonomy(url))
  }


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

  def extractTopics(input: RDD[String]): RDD[Map[String,Any]] = {
    input.map(line=>Map("line"->line, "topics"->calculateTopics(line)))

  }

  def extractTopicsFromRDD(input: RDD[String]): RDD[String] = {

    val orig = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]())).map(x => collection.mutable.Map(x.toSeq: _*)).map(x =>{
      val m = collection.mutable.Map(x.get("_source").asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]()).toSeq: _*)

      val new_x = x + ("_source" -> extractTopicsFromMap(m))

      new_x
    })

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

  /*

  def extractTopicsFromRDDofMap(input: RDD[scala.collection.mutable.Map[String,Any]]): RDD[scala.collection.mutable
  .Map[String,Any]] = {

    input.map(m => {
      m += ("topics"->calculateTopics(m.getOrElse("text","").asInstanceOf[String]))
    })

  }
  */


  /*

 def extractTopicsFromJSON(input: RDD[String]): RDD[String] = {

   val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]()).get
     ("_source").asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]())).map(x => collection.mutable.Map
     (x.toSeq: _*))

   val temp2 = extractTopicsFromRDD(temp)

   //val orig = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]())).map(x => collection.mutable.Map(x.toSeq: _*)).map(x => x)

   val result = temp2.mapPartitions(x=> {

     implicit val formats = Serialization.formats(NoTypeHints)

     var a = List[String]()

     while(x.hasNext) {
       val r = x.next()
       a.::=(write(r))
     }
     a.toIterator
   })

   // extractTopicsFromRDD(temp).map(x=>write(x))

   result


 }
 */

}


