/* SimpleApp.scala */

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import topic.TopicExtractor

object TopicExtractorAsLibraryApp {
  def main(args: Array[String]) {
    val logFile = "/home/cnavarro/spark-1.5.1-bin-hadoop2.6/README.md" // Should be some file on your system
    val taxonomy_url = new URL("file:///home/cnavarro/workspace/mixedemotions/spark_test/src/resources/example_taxonomy.json")
    val topicExtractor = new TopicExtractor(taxonomy_url)

    val conf = new SparkConf().setAppName("External Classificator Application")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(logFile, 2).cache()
    val topicMaps = lines.map(line=>Map("line"->line, "topics"->topicExtractor.calculateTopics(line)))
    for(topicMap <- topicMaps.collect()){
      println(topicMap)
    }

  }
}