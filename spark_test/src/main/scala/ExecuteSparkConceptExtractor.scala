import java.net.URL

import concept.SparkConceptExtractor
import org.apache.spark.{SparkConf, SparkContext}
import topic.SparkTopicExtractor

/**
 * Created by cnavarro on 9/10/15.
 */
object ExecuteSparkConceptExtractor {
  def main (args: Array[String]) {
    val logFile = "/home/cnavarro/spark-1.5.1-bin-hadoop2.6/README.md" // Should be some file on your system
    val taxonomy_url = new URL("file:///home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/example_taxonomy.json")
    val conceptExtractor = new SparkConceptExtractor("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/small_taxonomy.tsv")
    val conf = new SparkConf().setAppName("External Classificator Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(logFile, 2).cache()
    val conceptMaps = conceptExtractor.extractConceptsJson(lines)
    for(conceptMap <- conceptMaps.collect()){
      println(conceptMap)
    }

  }


}
