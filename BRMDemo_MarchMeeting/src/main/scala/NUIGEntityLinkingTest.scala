/*
import ie.nuig.entitylinking.core.AnnotatedMention
import ie.nuig.entitylinking.core.ELDocument
import ie.nuig.entitylinking.main.nel.EntityLinkingMain

import java.util.List
import scala.collection.JavaConversions._





object NUIGEntityLinkingTest {

  def main(args: Array[String]) {

    //config file path that contains paths for classifier and candidate index.
    val configPath = "/home/cnavarro/workspace/mixedemotions/entitylinking/ie.nuig.me.nel.properties"

    //initiate entity linking module, load classifier and open index
    val entityLinkingDemo : EntityLinkingMain = new EntityLinkingMain(configPath)

    //input text example
    val docText = "Apple CEO Steve Jobs and Baez dated in the late 1970s, and she performed at his Stanford memorial."

    //create an document that contains document text
    val elDocument : ELDocument = new ELDocument(docText, null)

    //process document to recognize the entities and link them to DBpedia
    entityLinkingDemo.processDocument(elDocument)

    //print results
    //EntityText, entityType, DBpediaLink, confidenceScore
    val annotatedMentions : List[AnnotatedMention] = elDocument.getAnnotatedMention().toList
    println(annotatedMentions.getClass)
    for(annotatedMention : AnnotatedMention <- annotatedMentions){
      println("Entity text: "+annotatedMention.getMention() + "\tclass: "+annotatedMention.getClassType() +"\tURI: "+
        annotatedMention.getUriScorePair().getKey() +"\tscore: "+annotatedMention.getUriScorePair().getValue())

    }

  }
}

*/


