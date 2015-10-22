package topic

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

  def main (args: Array[String]) {


    val path = new URL("file:///home/cnavarro/workspace/mixedemotions/spark_test/src/resources/example_taxonomy.json")
    val json_string = Source.fromFile("/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/example_taxonomy.json").getLines.mkString

    //val json_string = "{\n  \"competidoras\": [\n    \"COMPETENCIA\"\n  ],\n  \"reputacion\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"competidores\": [\n    \"COMPETENCIA\"\n  ],\n  \"arbitros\": [\n    \"FUTBOL\"\n  ],\n  \"competidora\": [\n    \"COMPETENCIA\"\n  ],\n  \"calidad\": [\n    \"CALIDAD\"\n  ],\n  \"pelota\": [\n    \"FUTBOL\"\n  ],\n  \"gasoleo\": [\n    \"COMBUSTIBLE\"\n  ],\n  \"defensas\": [\n    \"FUTBOL\"\n  ],\n  \"campa\\\\u00f1a de marketing\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"seguridad\": [\n    \"CALIDAD\"\n  ],\n  \"vestuario\": [\n    \"FUTBOL\"\n  ],\n  \"defensa\": [\n    \"FUTBOL\"\n  ],\n  \"innovacion\": [\n    \"INNOVACION\"\n  ],\n  \"portero\": [\n    \"FUTBOL\"\n  ],\n  \"relacion calidad precio\": [\n    \"CALIDAD\"\n  ],\n  \"gestor\": [\n    \"DIRECTIVOS\"\n  ],\n  \"jugador\": [\n    \"FUTBOL\"\n  ],\n  \"partido\": [\n    \"FUTBOL\"\n  ],\n  \"diesel\": [\n    \"COMBUSTIBLE\"\n  ],\n  \"gerente\": [\n    \"DIRECTIVOS\"\n  ],\n  \"jefe\": [\n    \"DIRECTIVOS\"\n  ],\n  \"innovacion y desarrollo\": [\n    \"INNOVACION\"\n  ],\n  \"anuncios\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"falta\": [\n    \"FUTBOL\"\n  ],\n  \"porteros\": [\n    \"FUTBOL\"\n  ],\n  \"copa\": [\n    \"FUTBOL\"\n  ],\n  \"relacion calidad-precio\": [\n    \"CALIDAD\"\n  ],\n  \"meses de garantia\": [\n    \"CALIDAD\"\n  ],\n  \"gol\": [\n    \"FUTBOL\"\n  ],\n  \"imagen de marca\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"investigacion\": [\n    \"INNOVACION\"\n  ],\n  \"director general\": [\n    \"DIRECTIVOS\"\n  ],\n  \"liga\": [\n    \"FUTBOL\"\n  ],\n  \"competencia\": [\n    \"COMPETENCIA\"\n  ],\n  \"garantia\": [\n    \"CALIDAD\"\n  ],\n  \"confianza\": [\n    \"CALIDAD\"\n  ],\n  \"ceo\": [\n    \"DIRECTIVOS\"\n  ],\n  \"representante\": [\n    \"DIRECTIVOS\"\n  ],\n  \"opinion online\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"arbitro\": [\n    \"FUTBOL\"\n  ],\n  \"futbolistas\": [\n    \"FUTBOL\"\n  ],\n  \"futbol\": [\n    \"FUTBOL\"\n  ],\n  \"investigacion y desarrollo\": [\n    \"INNOVACION\"\n  ],\n  \"gasolina\": [\n    \"COMBUSTIBLE\"\n  ],\n  \"competidor\": [\n    \"COMPETENCIA\"\n  ],\n  \"partidos\": [\n    \"FUTBOL\"\n  ],\n  \"i+d+i\": [\n    \"INNOVACION\"\n  ],\n  \"competitividad\": [\n    \"COMPETENCIA\"\n  ],\n  \"director\": [\n    \"DIRECTIVOS\"\n  ],\n  \"corner\": [\n    \"FUTBOL\"\n  ],\n  \"fuera de juego\": [\n    \"FUTBOL\"\n  ],\n  \"futbolista\": [\n    \"FUTBOL\"\n  ],\n  \"empresas competidoras\": [\n    \"COMPETENCIA\"\n  ],\n  \"relacion calidad y precio\": [\n    \"CALIDAD\"\n  ],\n  \"gas\": [\n    \"COMBUSTIBLE\"\n  ],\n  \"a\\\\u00f1os de garantia\": [\n    \"CALIDAD\"\n  ],\n  \"petroleo\": [\n    \"COMBUSTIBLE\"\n  ],\n  \"directora\": [\n    \"DIRECTIVOS\"\n  ],\n  \"i+d\": [\n    \"INNOVACION\"\n  ],\n  \"penalty\": [\n    \"FUTBOL\"\n  ],\n  \"campa\\\\u00f1a de publicidad\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"champion\": [\n    \"FUTBOL\"\n  ],\n  \"anuncio\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"balon\": [\n    \"FUTBOL\"\n  ],\n  \"goles\": [\n    \"FUTBOL\"\n  ],\n  \"relacion calidad / precio\": [\n    \"CALIDAD\"\n  ],\n  \"relacion calidad/precio\": [\n    \"CALIDAD\"\n  ],\n  \"jugadores\": [\n    \"FUTBOL\"\n  ],\n  \"publicidad\": [\n    \"IMAGEN DE MARCA\"\n  ],\n  \"spot publicitario\": [\n    \"IMAGEN DE MARCA\"\n  ]\n}"

    val taxonomy = JSON.parseFull(json_string).get.asInstanceOf[Map[String,List[String]]]
    println(taxonomy)
    //val classificationService = new ClassificationService(taxonomy)
    val classificationService = new TopicExtractor(path)
    val text  = "Florentino Fernández es el presidente del vestuario pese a su Reputación"
    val topics = classificationService.calculateTopics(text)
    println(topics)


  }
}