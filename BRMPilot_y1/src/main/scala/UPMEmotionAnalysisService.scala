import java.net.URLEncoder

import org.apache.spark.rdd.RDD

import scalaj.http.{HttpResponse, _}

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON


object UPMEmotionAnalysisService {

  // The elements of the original RDD are separately processed. the mapPartitions method is used to optimize performance
  def process(input: RDD[String]): RDD[String] = {
    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val emotioned = temp.mapPartitions(x => extractEmotions(x))

    emotioned.mapPartitions(x => {

      implicit val formats = Serialization.formats(NoTypeHints)

      var a = List[String]()
      while(x.hasNext) {
        val r = x.next()
        a=a:+(write(r))
      }
      a.toIterator

    })

  }

  def extractEmotions(input: Iterator[scala.collection.mutable.Map[String,Any]]) : Iterator[scala.collection.mutable.Map[String,Any]] = {
    for(entry<-input) yield {
      entry += ("emotions" -> extractEmotions(entry))
    }
  }

  def extractEmotions(input: scala.collection.mutable.Map[String,Any]) : scala.collection.mutable.Map[String,Any] = {
     val query = composeQuery(input)
      println(query)
      val response = NetworkAnalysisService.executeGetRequest(query)
      val emotions = getEmotionsFromResponse(response)
      collection.mutable.Map(emotions.toSeq: _*)
  }

  // Each request involves the composition of a query to the service. Inthis case, the query is delivered to the DW API
  def composeQuery(input: scala.collection.mutable.Map[String,Any]): String = {
    val encodedText = URLEncoder.encode(input("text").toString, "UTF-8")
    s"http://senpy.demos.gsi.dit.upm.es/api/?i=${encodedText}&lang=${input("lang")}&algo=EmoTextANEW"
  }


  def getEmotionsFromResponse(input: Map[String,Any]) : Map[String, Any] = {
    val entries = input.getOrElse("entries", List()).asInstanceOf[List[Map[String, Any]]]
    if(entries.length==0){
      Map()
    }else{
      val entry = entries.head
      val parent =  entry("emotions").asInstanceOf[List[Map[String,Any]]].head
      val emotionsDict = parent("onyx:hasEmotion").asInstanceOf[List[Map[String,Any]]].head
      val arousal = emotionsDict("http://www.gsi.dit.upm.es/ontologies/onyx/vocabularies/anew/ns#arousal").asInstanceOf[Double]
      val dominance = emotionsDict("http://www.gsi.dit.upm.es/ontologies/onyx/vocabularies/anew/ns#dominance").asInstanceOf[Double]
      val valence = emotionsDict("http://www.gsi.dit.upm.es/ontologies/onyx/vocabularies/anew/ns#valence").asInstanceOf[Double]
      val category = emotionsDict("onyx:hasEmotionCategory").asInstanceOf[String].replace("http://gsi.dit.upm.es/ontologies/wnaffect/ns#","")
      val emotion = category.replace("negative-","")
      val emotions = Map("arousal"->arousal, "dominance"-> dominance, "valence"->valence, "category"->category, "emotion"->emotion)
      emotions
    }
  }

  def main(args: Array[String]) {
    val inputs = Array("{\"text\": \"I hate western movies with John Wayne\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is really nasty. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is awesome, but maybe it is just for fans. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Hola ke ace?\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"La nueva de Star Wars está muy bien. Me encantó el robot pelota.\", \"nots\": [\"hola\"], \"lang\": \"es\"}")

    for(input<-inputs){
      val inputMap = JSON.parseFull(input).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())
      val mutableMap = collection.mutable.Map(inputMap.toSeq: _*)
      val query = composeQuery(mutableMap)
      println(query)
      val response = NetworkAnalysisService.executeGetRequest(query)
      val emotions = getEmotionsFromResponse(response)
      println(emotions)
    }

  }


}
