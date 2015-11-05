import java.net.URL

import org.apache.spark.{SparkFiles, SparkContext, SparkConf}
import topic.SparkTopicExtractor

import scala.io.Source
import scala.util.parsing.json.JSON

import concept.BasicConceptExtractorNoSerializable

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}


/**
 * Created by cnavarro on 22/10/15.
 */
object POCIntegration {
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

  def extractConceptsJson(lines: Iterator[String]): Iterator[String] = {
    val conceptExtractor = new BasicConceptExtractorNoSerializable("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/small_taxonomy.tsv")
    //    var result = new mutable.MutableList[Map[String, Any]]()
    //(lines).map { case line => Map("line" -> line, "concepts" -> conceptExtractor.extractConcepts(line.getOrElse("text","").asInstanceOf[String])) }
    val mapLines = lines.map(line=>line.map(entry=>JSON.parseFull(line).asInstanceOf[Some[Map[String,Any]]].get))
    for(line<-lines) yield {
      val lineMap = JSON.parseFull(line).asInstanceOf[Some[Map[String,Any]]].get
      val resultMap = lineMap + (("concepts",lineMap.getOrElse("text","").asInstanceOf[String]))
      write(resultMap)
    }


  }

  def calculateLength(input: String): String = {
    val jsonSomeInput = JSON.parseFull(input).asInstanceOf[Some[Map[String,Any]]]
    val jsonInput = jsonSomeInput.getOrElse(Map())
    val text = jsonInput.getOrElse("text", "").asInstanceOf[String]
    val length = text.length
    val outputJson = jsonInput+(("length",length))
    val output = write(outputJson)
    output

  }

  def cleanJsonOutput(input: JValue): String = {
    input.toString.replaceAll("""JString\(""","").replaceAll("""\)$""","")
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

    //Load docs
    val json_string = Source.fromFile("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/example_input_docs.json").getLines.mkString
    val exampleInput = JSON.parseFull(json_string).asInstanceOf[Some[Map[String,Any]]]
    val e2 = exampleInput.getOrElse(Map())
    val hits = e2.getOrElse("hits",Map()).asInstanceOf[Map[String,Any]]
    val docs = hits.getOrElse("hits", List(Map())).asInstanceOf[List[Map[String,Any]]]

    //Initialize Spark
    val conf = new SparkConf().setAppName("POC: Modules Integration in Spark").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val parallelRawDocs = sc.parallelize(docs)
    val parallelDocs = parallelRawDocs.map(_.getOrElse("fields",List() ).asInstanceOf[Map[String,List[String]]])

    //Spark approach: Extract topics
    val taxonomy_url = new URL("file:///home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/example_taxonomy.json")
    val topicExtractor = new SparkTopicExtractor(taxonomy_url)
    val flatParallelDocs = parallelDocs.flatMap(element=>flattenByText(element))
    val jsonParallelDocs = flatParallelDocs.map(write(_))
    val topicMaps = topicExtractor.extractTopicsFromRDD(jsonParallelDocs)

    println("Going to show topics first-----------------")
    println("Topic first: " +  topicMaps.first)



    //MapPartitions (non-serializable approach)
    val conceptMaps = topicMaps.mapPartitions(extractConceptsJson)
    println("Concepts first" + conceptMaps.first())


    val distScript = "./src/resources/count_words_stdin_json.rb"
    val distScriptName = "count_words_stdin_json.rb"
    sc.addFile(distScript)
    /*val distDependency = "./src/resources/dependency.rb"
    sc.addFile(distDependency)*/

    //val conceptJsons = conceptMaps.map(entry=>compact(write(entry)))
    //val conceptJsons = conceptMaps.map(entry=>write(entry))
    //println(conceptJsons.first())



    val counts = conceptMaps.pipe(SparkFiles.get(distScriptName))
    //conceptJsons.pipe("tee /home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/tee_hee.txt").collect()
    //val counts = conceptJsons.pipe("ruby /home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/count_words_stdin_json.rb")
    /*println("Whaaaaaaaaaaaaaaaaat uppppppppppppppppppppppppppp")
    val jsonToWrite = flatParallelDocs.map(_.getOrElse("text","")).pipe("tee /home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/tee_hee.txt")
    jsonToWrite.collect()*/

    val firstCount = counts.first()
    counts.cache()
    val collectedCounts = counts.collect
    println("Couuuuuuuuunts: "+ firstCount)
    println("Counts length: " + counts.collect.length)
    println("Counts class:" + firstCount.getClass())
    println("Couuuunts 10:" + collectedCounts.slice(0,10).mkString("\n"))
    println("Parsed " + parse(firstCount))
    println("Parsed " + JSON.parseFull(firstCount).asInstanceOf[Some[Map[String,Any]]] )



    val lengths = counts.map(entry=>calculateLength(entry))
    println("Lengths: " + lengths.first)



    //val mapCounts = conceptJsons.map(entry =>JSON.parseFull(entry).asInstanceOf[Some[Map[String,Any]]])
    //val mapCounts = conceptJsons.map(entry =>parse(entry))
    val mapCounts = counts.map(entry =>parse(entry))
    val oneJson = mapCounts.first()
    println("One json " + oneJson)
    println(oneJson \\ "text")





  }

}
