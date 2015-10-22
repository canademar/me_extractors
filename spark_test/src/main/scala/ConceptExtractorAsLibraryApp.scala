import java.net.URL

import concept.BasicConceptExtractor
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
 * Created by cnavarro on 13/10/15.
 */
object ConceptExtractorAsLibraryApp {
  def main(args: Array[String]) {
    println("Do not get it")

    val conceptExtractor = new BasicConceptExtractor("/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/small_taxonomy.tsv")
    println("Where art thou")
    val textFile = "/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/volkswagen.txt"
    val conf = new SparkConf().setAppName("External Classificator Application")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(textFile, 2).cache()
    println("Probably here")
    val conceptMaps = lines.map(line => Map("line" -> line, "topics" -> conceptExtractor.extractConcepts(line)))
    for (conceptMap <- conceptMaps.collect()) {
      println(conceptMap)
    }
    println("You have failed me")
  }
}

