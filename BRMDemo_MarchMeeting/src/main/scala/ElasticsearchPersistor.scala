import java.io.{File, PrintWriter}

import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import com.sksamuel.elastic4s.ElasticDsl.index
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.apache.spark.rdd.RDD
import org.elasticsearch.common.settings.ImmutableSettings

/**
 * Created by cnavarro on 16/02/16.
 */
class ElasticsearchPersistor(val client: ElasticClient) {

  def this(ip: String, port: Int, clusterName: String){
    this(ElasticClient.remote(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build(), ElasticsearchClientUri("elasticsearch://" + ip + ":9300")))
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


}

object ElasticsearchPersistor {

  def persistTweetsFromMapMP(lines: Iterator[scala.collection.mutable.Map[String,Any]], ip:String, port:Int, clusterName: String) : Iterator[scala.collection.mutable.Map[String,Any]]= {
    val persistor : ElasticsearchPersistor = new ElasticsearchPersistor(ip, port, clusterName)

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

  def persistTweetsFromMap(lines: Iterator[scala.collection.mutable.Map[String,Any]], ip:String, port:Int, clusterName: String) : Unit= {
    val persistor : ElasticsearchPersistor = new ElasticsearchPersistor(ip, port, clusterName)

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

  def persistTweetsFromRDDmp(input: RDD[String], ip: String, port: Int, clusterName: String): RDD[String] = {
    println("~~~~~~~~~~~~~~~~~going to persist")
    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = temp.mapPartitions(iter => persistTweetsFromMap(iter, ip, port, clusterName))


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


  def persistTweetsFromRDD(input: RDD[String], ip: String, port: Int, clusterName: String): Unit = {
    println("~~~~~~~~~~~~~~~~~going to persist")
    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = temp.foreachPartition(iter => persistTweetsFromMap(iter, ip, port, clusterName))




  }
}




