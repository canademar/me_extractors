//import java.util.List
/*
import edu.insight.unlp.nn.example.TwitterSentiMain
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.JavaConversions._

import ie.nuig.entitylinking.core.AnnotatedMention
import ie.nuig.entitylinking.core.ELDocument
import ie.nuig.entitylinking.main.nel.EntityLinkingMain

import scala.util.parsing.json.JSON


class NUIGEntityLinkingExtractor(entityLinkingModel: EntityLinkingMain) {

  def this(confPath: String){
    this(new EntityLinkingMain(confPath))
  }

  def extractConcepts(text: String): List[Map[String, String]] ={
    //create an document that contains document text
    val elDocument : ELDocument = new ELDocument(text, null)

    //process document to recognize the entities and link them to DBpedia
    entityLinkingModel.processDocument(elDocument)

    val annotatedMentions : List[AnnotatedMention] = elDocument.getAnnotatedMention().toList

    formatMentions(annotatedMentions)


  }

  def formatMentions(mentions: List[AnnotatedMention]): List[Map[String, String]] = {
    mentions match{
      case x::xs => formatMention(x)::formatMentions(xs)
      case Nil => List()

    }

  }

  def formatMention(mention: AnnotatedMention) : Map[String,String] = {
    //println("Entity text: "+ mention.getMention() + "\tclass: "+ mention.getClassType() +"\tURI: "+
    //    mention.getUriScorePair().getKey() +"\tscore: "+ mention.getUriScorePair().getValue())

    Map("entity" -> mention.getMention(), "entityClass" -> mention.getClassType,
      "uri" -> mention.getUriScorePair().getKey(), "score" -> mention.getUriScorePair().getValue().toString)





  }

}

object NUIGEntityLinkingExtractor {

  //val confPath = "/home/cnavarro/workspace/mixedemotions/entitylinking/ie.nuig.me.nel.properties"

 def extractEntityLinkingFromMap(lines: Iterator[scala.collection.mutable.Map[String,Any]], confPath: String) : Iterator[scala.collection.mutable
  .Map[String,Any]]= {

    val entityLinker : NUIGEntityLinkingExtractor = new NUIGEntityLinkingExtractor(confPath)

    for(line<-lines) yield {
      val text = line.getOrElse("text","").asInstanceOf[String]
      val lang = line.getOrElse("lang","").asInstanceOf[String]
      if(text.length>0 && lang.equals("en")){
        //line + (("sentiment", sentimenter.classify(text)))
        //line += ("sentiment"-> sentimenter.classify(text))
        line += ("entities" -> entityLinker.extractConcepts(text))
        //Map("_source"->(line.getOrElse("_source", Map[String,String]()).asInstanceOf[Map[String,String]] + (("sentiment", sentimenter.classify(text)))))
      }else{
        line += ("entities"-> Map())
      }
    }


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
*/