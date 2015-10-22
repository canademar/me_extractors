import java.net.URL

import org.apache.spark.{SparkContext, SparkConf}
import topic.SparkTopicExtractor

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Created by cnavarro on 22/10/15.
 */
object POCIntegration {

  def flattenByText(map :Map[String,Any]): List[Map[String,Any]] ={
    val textList = map.getOrElse("text",List()).asInstanceOf[List[String]]
    for(text<-textList) yield map-"text"+(("text",text))
  }

  /*
   * Extract input data from ES (optional)
   * Convert to RDD
   * Pass some algorithms with every strategy
   * * Spark -> Topic Extractor
   * * Scala Mappeable -> Write something
   * * Scala non mappeable -> ConceptExtractor
   * * Pipe -> count_words.rb?
   * * REST -> Find something
   */
  def main (args: Array[String]) {

    val json_string = Source.fromFile("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/example_input_docs.json").getLines.mkString

    //val exampleInput = JSON.parseFull(json_string).get.asInstanceOf[Map[String,Map]]
    val exampleInput = JSON.parseFull(json_string).asInstanceOf[Some[Map[String,Any]]]
    val e2 = exampleInput.getOrElse(Map())

    val hits = e2.getOrElse("hits",Map()).asInstanceOf[Map[String,Any]]
    println(hits.getClass)
    val docs = hits.getOrElse("hits", List(Map())).asInstanceOf[List[Map[String,Any]]]


    val conf = new SparkConf().setAppName("POC: Modules Integration in Spark").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val parallelRawDocs = sc.parallelize(docs)
    val parallelDocs = parallelRawDocs.map(_.getOrElse("fields",List() ).asInstanceOf[Map[String,List[String]]])

    val taxonomy_url = new URL("file:///home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/example_taxonomy.json")
    val topicExtractor = new SparkTopicExtractor(taxonomy_url)
    val flatParallelDocs = parallelDocs.flatMap(element=>flattenByText(element))
    val topicMaps = topicExtractor.extractTopicsFromRDD(flatParallelDocs)

    //for(source<-parallelDocs.collect()){
    //  println("Doc:" + source)
    //}
    print(topicMaps.first())
    




  }

}
