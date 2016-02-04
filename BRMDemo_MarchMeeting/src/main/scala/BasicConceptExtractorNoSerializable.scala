import java.net.URL
import java.text.Normalizer

import scala.io.Source


/**
 * Created by cnavarro on 8/10/15.
 */
class BasicConceptExtractorNoSerializable(taxonomy: Map[String, Int], inlinks_threshold: Int, window_max_length: Int){

  def this(path: URL) {
    this(BasicConceptExtractorNoSerializable.parseTaxonomy(path), 400, 10)
  }

  def extractConcepts(text: String): Set[String] ={
    var start = 0
    var length = 1
    var concepts = scala.collection.mutable.Set[String]()
    val clean_text = BasicConceptExtractorNoSerializable.cleanText(text)
    val words = clean_text.split("""[\s\.\n"',()]""")
    while(start+length<words.length){
      length = 1
      while(length<=window_max_length) {

        val phrase = words.slice(start, start + length).mkString(" ")

        taxonomy.get(phrase) match {
          case Some(i: Int) => {
            if (i > inlinks_threshold) {
              concepts += phrase
            }
          }
          case None =>{

            }
        }
        length += 1
      }
      start += 1
    }
    concepts.toSet
  }



}

object BasicConceptExtractorNoSerializable{
  def parseTaxonomy(path: URL): Map[String, Int] ={
    var taxonomy = scala.collection.mutable.Map[String, Int]()
    val file = Source.fromURL(path,"iso-8859-1")
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



}

