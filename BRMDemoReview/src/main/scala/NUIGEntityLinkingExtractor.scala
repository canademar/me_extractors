//import java.util.List

import ie.nuig.entitylinking.core.{AnnotatedMention, ELDocument}
import ie.nuig.entitylinking.main.nel.EntityLinkingMain
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON

// For the integration of the EntityLinking module (https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html):
// i) To download lucene-4.10.0
// ii) To copy "lucene-core-4.10.0.jar" to a directory in the machine from which the spark-submit is launched
// iii) To specify the use of this JAR in the SparkSubmit command: --jars (path)/lucene-core-4.10.0.jar

class NUIGEntityLinkingExtractor(entityLinkingModel: EntityLinkingMain) {

  def this(confPath: String){
    this(new EntityLinkingMain(confPath))
  }

  def extractConcepts(text: String): List[Map[String, String]] ={
    //create an document that contains document text
    val elDocument : ELDocument = new ELDocument(text, null)

    //process document to recognize the entities and link them to DBpedia
    entityLinkingModel.processDocument(elDocument)

    val annotatedMentions = elDocument.getAnnotatedMention().toArray.toList.map(e=>e.asInstanceOf[AnnotatedMention])

    formatMentions(annotatedMentions)

  }

  def formatMentions(mentions: List[AnnotatedMention]): List[Map[String, String]] = {
    mentions match {
      case x::xs => formatMention(x)::formatMentions(xs)
      case Nil => List()
    }

  }

  def formatMention(mention: AnnotatedMention) : Map[String,String] = {

    Map("entity" -> mention.getMention(), "entityClass" -> mention.getClassType,
      "uri" -> mention.getUriScorePair().getKey(), "score" -> mention.getUriScorePair().getValue().toString)

  }

}

object NUIGEntityLinkingExtractor {

 def extractEntityLinkingFromMap(lines: Iterator[scala.collection.mutable.Map[String,Any]], confPath: String) : Iterator[scala.collection.mutable
  .Map[String,Any]]= {

   val entityLinker: NUIGEntityLinkingExtractor = new NUIGEntityLinkingExtractor(confPath)

   lines.map(line => {
     val text = line.getOrElse("text", "").asInstanceOf[String]
     val lang = line.getOrElse("lang", "").asInstanceOf[String]
     if (text.length > 0 && lang.equals("en")) {
       line += ("entities" -> entityLinker.extractConcepts(text))
     } else {
       line += ("entities" -> Map())
     }
   })
 }


  def extractEntityLinkingFromRDD(input: RDD[String], confPath: String): RDD[String] = {

    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = temp.mapPartitions(item =>extractEntityLinkingFromMap(item, confPath))

    temp2.mapPartitions(x => {

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
