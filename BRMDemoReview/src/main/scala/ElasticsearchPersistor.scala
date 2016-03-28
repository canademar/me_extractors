import com.sksamuel.elastic4s.ElasticDsl.{bulk, index, _}
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.apache.spark.rdd.RDD
import org.elasticsearch.common.settings.ImmutableSettings
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON

/**
 * Created by cnavarro on 16/02/16.
 */
class ElasticsearchPersistor(val client: ElasticClient, val indexName: String) {

  def this(ip: String, port: Int, clusterName: String, indexName: String){
    this(ElasticClient.remote(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build(),
      ElasticsearchClientUri("elasticsearch://" + ip + ":9300")),     indexName)
  }


  def saveTweet(tweet : Map[String, Any]) :  scala.collection.mutable.Map[String, Any] ={
    println("Saving tweets")


    val resp = client.execute {
      val rawTweet  = tweet("raw").asInstanceOf[Map[String,Any]]
      index into "myanalyzed" / "tweet" fields(
        "lang" -> tweet("lang").asInstanceOf[String],
        "raw" ->  rawTweet,
        "brand" -> tweet("brand").asInstanceOf[String],
        "text" -> tweet("text").asInstanceOf[String],
        "created_at" -> tweet("time").asInstanceOf[String],
        //"hashtags" -> record.getOrElse("hashtags", "").asInstanceOf[List[String]].toArray,
        //"topics" -> record.getOrElse("topics", "").asInstanceOf[List[String]].toArray,
        "project" -> tweet("project_id").asInstanceOf[Double].round.toInt,
        //"concepts" -> record.getOrElse("concepts", "").asInstanceOf[List[String]].toArray,
        //"mentions" -> record.getOrElse("mentions", "").asInstanceOf[List[String]].toArray,
        //"sentiment" -> record.getOrElse("sentiment", "").asInstanceOf[String],
        "id" -> rawTweet("id_str").asInstanceOf[String],
        "url" -> tweet("url"),
        "synonym_found" -> tweet("synonym_found"),
        "source" -> tweet("source"),
        "nots" -> tweet("nots"),
        "synonyms" -> tweet("synonyms"),
        "index_time" -> System.currentTimeMillis()

        ) id rawTweet("id")
    }.await // don't block in real code

    collection.mutable.Map(tweet.toSeq: _*)


    /*
    val pw = new PrintWriter(new File("/home/cnavarro/ids.txt" ))
    pw.write(""+record.getOrElse("id", "").asInstanceOf[Double])
    pw.write("\n")
    pw.write(""+result.getOrElse("_id", "").asInstanceOf[String])
    pw.write("\n")
    */


  }

  def saveTweets(tweets: Seq[Map[String,Any]]): Unit ={
    println("Bulk Bogan!")
     val resp = client.execute {
       bulk(
         for(tweet<-tweets) yield {
           index into indexName / "tweet" fields (tweet) id tweet("id")
         }
           )
       }

     }




}

object ElasticsearchPersistor {

  def persistTweetsFromMapMP(lines: Iterator[scala.collection.mutable.Map[String,Any]], ip:String, port:Int,
                             clusterName: String, indexName: String) : Iterator[scala.collection.mutable.Map[String,Any]]= {
    val persistor : ElasticsearchPersistor = new ElasticsearchPersistor(ip, port, clusterName, indexName )

    for(line<-lines) yield {
      persistor.saveTweet(line.toMap)
    }

    /*
    var todosloselements: Array
    foreach 100 elements in lines
       todosloseleemtns += persist(100elements)

    return todosloselements.toIterator
     */



  }

  def persistTweetsFromMap(lines: Iterator[Map[String,Any]], ip:String, port:Int, clusterName: String,
                            indexName: String) : Unit= {
    val persistor : ElasticsearchPersistor = new ElasticsearchPersistor(ip, port, clusterName, indexName)

    val chunks = lines.grouped(100)

    for(chunk<-chunks) {
      persistor.saveTweets(chunk)
    }

  }

  def persistTweetsFromRDDmp(input: RDD[String], ip: String, port: Int, clusterName: String,
                              indexName: String): RDD[String] = {
    println("~~~~~~~~~~~~~~~~~going to persist")
    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = temp.mapPartitions(iter => persistTweetsFromMapMP(iter, ip, port, clusterName, indexName))


    temp2.mapPartitions(x => {

      implicit val formats = Serialization.formats(NoTypeHints)

      var a = List[String]()
      while(x.hasNext) {
        val r = x.next()
        a=a:+(write(r))
      }
      a.toIterator

    })

  }

  def formatTweet(tweet: Map[String,Any]) : Map[String, Any] = {


    val rawTweet  = tweet("raw").asInstanceOf[Map[String,Any]]
    val projectId = tweet("project_id").asInstanceOf[Double].round.toInt
    Map("lang" -> tweet("lang").asInstanceOf[String],
        "raw" ->  rawTweet,
        "brand" -> tweet("brand").asInstanceOf[String],
        "text" -> tweet("text").asInstanceOf[String],
        "created_at" -> tweet("time").asInstanceOf[String],
        //"hashtags" -> record.getOrElse("hashtags", "").asInstanceOf[List[String]].toArray,
        "topics" -> tweet.getOrElse("topics", List()).asInstanceOf[List[String]].toArray,
        "project" -> projectId,
        "concepts" -> tweet.getOrElse("concepts", List()).asInstanceOf[List[String]].toArray,
        //"mentions" -> record.getOrElse("mentions", "").asInstanceOf[List[String]].toArray,
        "emotions" -> tweet.getOrElse("emotions", Map()).asInstanceOf[Map[String,Any]],
        "sentiment" -> tweet.getOrElse("sentiment", "").asInstanceOf[Double],
        "polarity" -> tweet.getOrElse("polarity", "").asInstanceOf[String],
        "tweet_id" -> rawTweet("id_str").asInstanceOf[String],
        "id" -> List(projectId, rawTweet("id_str").asInstanceOf[String]).mkString("_"),
        "url" -> tweet("url"),
        "synonym_found" -> tweet("synonym_found"),
        "source" -> tweet("source"),
        "nots" -> tweet("nots"),
        "synonyms" -> tweet("synonyms"),
        "index_time" -> System.currentTimeMillis()
    )


  }


  def persistTweetsFromRDD(input: RDD[String], ip: String, port: Int, clusterName: String,
                           indexName: String): RDD[String] = {
    println("~~~~~~~~~~~~~~~~~going to persist in " + "ip" + port.toString + "clusterName")


    val parsedTweets = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]()))

    val formattedTweets = parsedTweets.map(tweet => formatTweet(tweet))

    formattedTweets.foreachPartition(iter => persistTweetsFromMap(iter, ip, port, clusterName, indexName))

    input
  }

  def persistTweetsWithoutSpark(input: List[String], ip: String, port: Int, clusterName: String,
                           indexName: String): Unit = {
    println("~~~~~~~~~~~~~~~~~going to persist in " + "ip" + port.toString + "clusterName")


    val parsedTweets = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]()))

    val formattedTweets = parsedTweets.map(tweet => formatTweet(tweet))

    val persistor : ElasticsearchPersistor = new ElasticsearchPersistor(ip, port, clusterName, indexName)

    val chunks = formattedTweets.grouped(100)

    for(chunk<-chunks) {
      persistor.saveTweets(chunk)
    }

  }


  def main (args: Array[String]) {
    val ip = "mixednode2"
    val port = 9300
    val clusterName = "Mixedemotions Elasticsearch"
    val indexName = "myanalyzed"
    val persistor : ElasticsearchPersistor = new ElasticsearchPersistor(ip, port, clusterName, indexName)
    val resp = persistor.client.execute {
      index into "myanalyzed" / "test" fields(

        "brand" -> "test",
        "text" -> "some text",
        "index_time" -> System.currentTimeMillis()

        ) id "test1111"
    }.await

  }
}




