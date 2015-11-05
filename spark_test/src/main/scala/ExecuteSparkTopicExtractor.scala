import java.net.URL

import org.apache.spark.{SparkContext, SparkConf}
import topic.SparkTopicExtractor

/**
 * Created by cnavarro on 9/10/15.
 */
object ExecuteSparkTopicExtractor {
  def main (args: Array[String]) {
    val logFile = "/home/cnavarro/spark-1.5.1-bin-hadoop2.6/README.md" // Should be some file on your system
    val taxonomy_url = new URL("file:///home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/example_taxonomy.json")
    val topicExtractor = new SparkTopicExtractor(taxonomy_url)
    val conf = new SparkConf().setAppName("External Classificator Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(logFile, 2).cache()
    val topicMaps = topicExtractor.extractTopics(lines)
    for(topicMap <- topicMaps.collect()){
      println(topicMap)
    }

  }


}
