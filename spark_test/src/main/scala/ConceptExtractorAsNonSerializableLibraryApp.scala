import concept.BasicConceptExtractorNoSerializable
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
 * Created by cnavarro on 13/10/15.
 */
object ConceptExtractorAsNonSerializableLibraryApp {

  def extractConcepts(lines: Iterator[String]): Iterator[Map[String,Any]] = {
    val conceptExtractor = new BasicConceptExtractorNoSerializable("/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/small_taxonomy.tsv")
//    var result = new mutable.MutableList[Map[String, Any]]()
    (lines).map { case line => Map("line" -> line, "concepts" -> conceptExtractor.extractConcepts(line)) }

  }



  def main(args: Array[String]) {
    //val conceptExtractor = new BasicConceptExtractorNoSerializable("/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/small_taxonomy.tsv")
    val textFile = "/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/volkswagen.txt"

    val conf = new SparkConf().setAppName("External Classificator Application")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(textFile, 2).cache()
    val conceptMaps = lines.mapPartitions(extractConcepts)
    for (conceptMap <- conceptMaps.collect()) {
      println(conceptMap)
    }
  }
}

