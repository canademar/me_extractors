import java.net.URL
import java.text.Normalizer

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.settings.ImmutableSettings
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.util.parsing.json.JSON

object MainDemo {

  implicit val formats = Serialization.formats(NoTypeHints)

  //val ip_host = "136.243.53.82" // IP address of the elasticsearch node
  val ip_host = "192.168.1.12" // Private IP address of the elasticsearch node
  val cluster_name = "elasticsearch" // Name of the Elasticsearch cluster
  val es_index = "tweets" // Name of the source index
  //val lang = "en" // Language of the documents to be retrieved
  val lang = "(es OR en)" // Language of the documents to be retrieved

  def main (args: Array[String]) {

    val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("mesos://192.168.1.12:5050") //setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Getting a client for ES
    val uri = ElasticsearchClientUri("elasticsearch://" + ip_host + ":9300")
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build()
    val client = ElasticClient.remote(settings, uri)

    var jsonLines = sc.parallelize(Array[String]())

    var z = 0

    var i = 0

    println("*** Quering Elasticsearch")

    while(z == 0) {
      // 1. Loading data from ES

      val elastic_query = "keyword:BBVA AND lang:" + lang
      println(elastic_query)
      val resp = client.execute {
        search in es_index / "tweet" query elastic_query limit 5000 start 5000*i
      }.await // don't block in real code

      if (resp.getHits.getHits.size <= 0) {
        z = 1
      }


      val arrayData = JSON.parseFull(resp.toString).asInstanceOf[Some[Map[String, Any]]].getOrElse(Map[String, Any]())
        .get("hits").asInstanceOf[Some[Map[String, Any]]].getOrElse(Map[String, Any]()).get("hits")
        .asInstanceOf[Some[List[Map[String, Any]]]].getOrElse(List(Map[String, Any]())).toArray.map(x => write(x))

      val newLines = sc.parallelize(arrayData)

      jsonLines = jsonLines.union(newLines)

      i = i + 1

      println("\nIteration: " + i + " - Total hits: " + resp.getHits.getHits.size + " - z = " + z + "\n")

    }

    client.close()

    // 2. Processing the items using NLP methods

    // 2.1. Processing the RDD with a NLP functionality implemented on Spark

    ////////////// TOPIC EXTRACTION (SPARK METHOD) ////////////////
    println("*** Topic Extraction")

    val mySparkTopicExtractor = new SparkTopicExtractor(new URL("http://136.243.53.81/repository/example_taxonomy.json"))

    val jsonResultTopic = mySparkTopicExtractor.extractTopicsFromRDD(jsonLines)

    ////////////// CONCEPT EXTRACTION (SPARK METHOD) ////////////////

    println("*** Concept Extraction")

    val tax = parseTaxonomy(sc, "hdfs://192.168.1.13:8020/user/stratio/resources/pagelinks_all.tsv")
    //val tax = parseTaxonomy(sc, "/home/jvmarcos/Escritorio/pagelinks_all.tsv") // Loading from local source

    val conceptExtractor = new SparkConceptExtractor(tax, 400, 10)

    val conceptResultString = conceptExtractor.extractConceptsFromRDD(jsonResultTopic)

    ////Sentiment Extraction

    println("*** Sentiment Extraction")

    val sentimentResultsString = SparkSentiment.extractSentimentFromRDD(conceptResultString)



    val finalResultMap = sentimentResultsString.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse
      (Map[String, Any]()).get("_source").asInstanceOf[Some[Map[String,Any]]].get)

    val example = sentimentResultsString.first()

    println("Example: "+ example)

    println("FinalResult: "+ finalResultMap.first())

    // 3. Writing documents in the database

    println("*** Persisting in ES " + finalResultMap.count())

    finalResultMap.foreachPartition( x => {

    val uri2 = ElasticsearchClientUri("elasticsearch://" + ip_host + ":9300")
    val settings2 = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build()
    val client2 = ElasticClient.remote(settings2, uri2)

      while(x.hasNext) {

        val record = x.next()

        val resp2 = client2.execute {
          index into "myanalyzed" / "tweet" fields(
            "lang" -> record.getOrElse("lang", "").asInstanceOf[String],
            "screen_name" -> record.getOrElse("screen_name", "").asInstanceOf[String],
            "keyword" -> record.getOrElse("keyword", "").asInstanceOf[String],
            "text" -> record.getOrElse("text", "").asInstanceOf[String],
            "created_at" -> record.getOrElse("created_at", "").asInstanceOf[String],
            "hashtags" -> record.getOrElse("hashtags", "").asInstanceOf[List[String]].toArray,
            "topics" -> record.getOrElse("topics", "").asInstanceOf[List[String]].toArray,
            "project" -> record.getOrElse("project", "").asInstanceOf[String],
            "concepts" -> record.getOrElse("concepts", "").asInstanceOf[List[String]].toArray,
            "mentions" -> record.getOrElse("mentions", "").asInstanceOf[List[String]].toArray,
            "sentiment" -> record.getOrElse("sentiment", "").asInstanceOf[String],
            "id" -> record.getOrElse("id", "").asInstanceOf[Double]

            ) id record.getOrElse("id", "").asInstanceOf[Double]
        }.await // don't block in real code
      }

      client2.close()

    })

  }

  def parseTaxonomy(sc: SparkContext, path: String): RDD[(String, Int)] ={
    var taxonomy = scala.collection.mutable.Map[String, Int]()
    val lines = sc.textFile(path).map(line=> {
      val parts = removeAccents(line.toLowerCase).split("\t")
      val concept = parts(0)
      val inlinks = parts(1).toInt
      (concept, inlinks)
    })

    lines

  }

  def removeAccents(text: String): String ={
    val str = Normalizer.normalize(text,Normalizer.Form.NFD)
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    exp.replaceAllIn(str,"")
  }


}
