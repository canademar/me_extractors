package concept

import java.text.Normalizer

import org.slf4j.LoggerFactory

import scala.io.Source;

/**
 * Created by cnavarro on 8/10/15.
 */
class BasicConceptExtractor(taxonomy: Map[String, Int], inlinks_threshold: Int, window_max_length: Int) extends Serializable{

  val logger = LoggerFactory.getLogger(BasicConceptExtractor.getClass);


  def this(path: String) {
    this(BasicConceptExtractor.parseTaxonomy(path), 400, 10)
  }

  def extractConcepts(text: String): Set[String] ={
    var start = 0
    var length = 1
    var concepts = scala.collection.mutable.Set[String]()
    val clean_text = BasicConceptExtractor.cleanText(text)
    val words = clean_text.split("""[\s\.\n"',()]""")
    while(start+length<words.length){
      length = 1
      while(length<=window_max_length && length<=(words.length-start)) {
        val phrase = words.slice(start, start + length).mkString(" ")
        logger.info(phrase)
        println("Phrase: " + phrase + " -- start: "+start + " length: "+ length)
        taxonomy.get(phrase) match {
          case Some(i: Int) => {
            if (i > inlinks_threshold) {
              logger.info("Adding {} to concepts", phrase)
              concepts += phrase
            }
          }
          case None =>{
            logger.info("Skipping ")

            }
        }
        length += 1
      }
      start += 1
      length = 1
    }
    for(concept<-concepts){
      for(otherConcept<-concepts){
        if(concept!=otherConcept & (concept contains otherConcept)){
          concepts.-(otherConcept)
        }
      }
    }
    concepts.toSet
  }



}

object BasicConceptExtractor{
  def parseTaxonomy(path: String): Map[String, Int] ={
    var taxonomy = scala.collection.mutable.Map[String, Int]()
    val file = Source.fromFile(path,"iso-8859-1")
    val lines = file.getLines()
    for(line<-lines){
      val parts = removeAccents(line.toLowerCase).split("\t")
      val concept = parts(0)
      val inlinks = parts(1).toInt
      taxonomy(concept) = inlinks
    }
    taxonomy.toMap
  }

  def cleanText(text: String): String = {
    var cleanText = removeAccents(text.toLowerCase)
    //cleanText = cleanText.replaceAll("\n", " ")
    cleanText = cleanText.replaceAll("""\s+""", " ")
    cleanText
  }

  def removeAccents(text: String): String ={
    val str = Normalizer.normalize(text,Normalizer.Form.NFD)
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    exp.replaceAllIn(str,"")
  }

  def main (args: Array[String]) {
    val conceptExtractor = new concept.BasicConceptExtractor("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/small_taxonomy.tsv")
    //val text = Source.fromFile("/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/volkswagen.txt").getLines().mkString(" ")
    val topics = conceptExtractor.extractConcepts("no volkswagen Que pasa con soria bale tudo")
    println("String: " + topics.mkString(","))
  }

}
