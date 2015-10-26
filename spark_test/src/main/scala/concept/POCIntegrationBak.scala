package concept

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import topic.SparkTopicExtractor

import scala.io.Source
import scala.util.parsing.json.JSON


/**
 * Created by cnavarro on 22/10/15.
 */
object POCIntegrationBak {
  implicit val formats = Serialization.formats(NoTypeHints)

  def flattenByText(map :Map[String,Any]): List[Map[String,Any]] ={
    val textList = map.getOrElse("text",List()).asInstanceOf[List[String]]
    for(text<-textList) yield map-"text"+(("text",text))
  }

  def extractConcepts(lines: Iterator[Map[String,Any]]): Iterator[Map[String,Any]] = {
    val conceptExtractor = new BasicConceptExtractorNoSerializable("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/small_taxonomy.tsv")
    //    var result = new mutable.MutableList[Map[String, Any]]()
    //(lines).map { case line => Map("line" -> line, "concepts" -> conceptExtractor.extractConcepts(line.getOrElse("text","").asInstanceOf[String])) }
    (lines).map { case line => line+(("concepts",conceptExtractor.extractConcepts(line.getOrElse("text","").asInstanceOf[String])))  }

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
    val conceptMaps = topicMaps.mapPartitions(extractConcepts)
    print(conceptMaps.first())

    /*val distScript = "./src/resources/count_words_stdin_json.rb"
    val distScriptName = "count_words_stdin_json.rb"
    sc.addFile(distScript)
    val distDependency = "./src/resources/dependency.rb"
    sc.addFile(distDependency)*/

    val conceptJsons = conceptMaps.map(entry=>compact(write(entry)))
    println(conceptJsons.first())

    //val counts = conceptJsons.pipe(SparkFiles.get(distScriptName))
    val counts = conceptJsons.pipe("ruby /home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/count_words_stdin_json.rb")

    val firstCount = counts.first()
    println("Couuuuuuuuunts: "+ firstCount)
    println("Counts class:" + firstCount.getClass())
    println("Parsed " + parse(firstCount))
    println("fullParse " + JSON.parseFull(firstCount).asInstanceOf[Some[Map[String,Any]]])






  }

}
