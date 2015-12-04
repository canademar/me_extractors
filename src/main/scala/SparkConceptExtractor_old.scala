import java.net.URL
import java.text.Normalizer

import org.apache.spark.rdd.RDD

/**
 * Created by cnavarro on 26/10/15.
 */
class SparkConceptExtractor_old (taxonomy: Map[String, Int], inlinks_threshold: Int, window_max_length: Int) extends Serializable{

  def this(path: URL) {
    this(BasicConceptExtractorNoSerializable.parseTaxonomy(path), 2, 10)
  }


  def extractConcepts(inputRDD: RDD[Map[String,Any]]): RDD[Map[String, Any]] ={
    inputRDD.map(line=>line+(("concepts", extractConceptForEntry(line.getOrElse("text","").asInstanceOf[String]))))

  }

  def extractConceptForEntry(text: String): List[String] ={
    var start = 0
    var length = 1
    var concepts = scala.collection.mutable.Set[String]()
    val clean_text = BasicConceptExtractor.cleanText(text)
    //println("\n\nClean text: " + clean_text)
    val words = clean_text.split("""[\s\.\n"',()]""")
    //words.foreach(w => println("Word: " + w))

    while(start+length<=words.length){
      length = 1
      while(length<=window_max_length && length<=(words.length-start)) {
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
      length = 1
    }
    for(concept<-concepts){
      for(otherConcept<-concepts){
        if(concept!=otherConcept & (concept contains otherConcept)){
          concepts.-(otherConcept)
        }
      }
    }


    concepts.toList

  }



}

object SparkConceptExtractor_old{
  def cleanText(text: String): String ={
    val cleanText = removeAccents(text.toLowerCase)
    cleanText.replaceAll("""\s+""", " ")
  }

  def removeAccents(text: String): String ={
    val str = Normalizer.normalize(text,Normalizer.Form.NFD)
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    exp.replaceAllIn(str,"")
  }
/*
  def main(args: Array[String]) {
    val conceptExtractor = new SparkConceptExtractor("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/small_taxonomy.tsv")
    val textFile = "/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/volkswagen.txt"

    val conf = new SparkConf().setAppName("Spark Concept Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(textFile, 2).cache()
    println("Probably here")
    val maps = lines.map(line=> Map("text"->line).asInstanceOf[Map[String,Any]])
    val conceptMaps = conceptExtractor.extractConcepts(maps)
    for (conceptMap <- conceptMaps.collect()) {
      println(conceptMap)
    }
    println("You have failed me")

  }
  */


}
