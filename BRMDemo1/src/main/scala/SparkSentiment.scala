import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON

import edu.insight.unlp.nn.example.TwitterSentiMain

/**
 * Created by cnavarro on 24/11/15.
 */
object SparkSentiment {
  val resourcesPath: String = "/opt/sds/me_resources/nuig_sentiment/resources/"
  val glovePath: String = resourcesPath + "embeddings/glove.twitter.27B.50d.txt"
  val datapath: String = resourcesPath + "data/twitterSemEval2013.tsv"
  val modelPath: String = resourcesPath + "model/learntSentiTwitter.model"

  def extractSentiment(lines: Iterator[String]): Iterator[Map[String,Any]] = {
    val timestart = System.currentTimeMillis()
    val sentimenter: TwitterSentiMain = new TwitterSentiMain
    sentimenter.loadModel(modelPath, glovePath, datapath)
    val timeend = System.currentTimeMillis()
    println("Time to initialize model: " + (timeend - timestart))
    //    var result = new mutable.MutableList[Map[String, Any]]()
    (lines).map { case line => Map("line" -> line, "sentiment" -> sentimenter.classify(line)) }


  }

  def extractSentimentFromJson(lines: Iterator[Map[String,Any]]) : Iterator[Map[String,Any]]= {


    val sentimenter: TwitterSentiMain = new TwitterSentiMain
    sentimenter.loadModel(modelPath, glovePath, datapath)


    //    var result = new mutable.MutableList[Map[String, Any]]()
    for(line<-lines) yield {
      val text = line.getOrElse("_source", "").asInstanceOf[Map[String, Any]].getOrElse("text","").asInstanceOf[String]
      val lang = line.getOrElse("_source", "").asInstanceOf[Map[String, Any]].getOrElse("lang","").asInstanceOf[String]
      if(text.length>0 && lang.equals("en")){
        //line + (("sentiment", sentimenter.classify(text)))
        Map("_source"->(line.getOrElse("_source", Map[String,String]()).asInstanceOf[Map[String,String]] + (("sentiment", sentimenter.classify(text)))))
      }else{
        line
      }
    }


  }

  def mapPartitionExample(iterator: Iterator[Map[String,Any]]) :Iterator[Map[String,Any]] = {
    for(item<-iterator) yield item
  }



  def extractSentimentFromRDD(input: RDD[String]): RDD[String] = {

    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]()).asInstanceOf[Map[String,Any]])

    val temp2 = temp.mapPartitions(item =>extractSentimentFromJson(item))




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



  def main(args: Array[String]) {

    val timestart = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("External Classificator Application")
      //.setMaster("local[2]")
      .setMaster("mesos://192.168.1.12:5050")
    val sc = new SparkContext(conf)
    //val lines = sc.parallelize(List("I definitely suggest the product, if you buy this then go for this also.", "I definitely hate this product. I did not like it at all."))
    //val lines = sc.textFile("resources/sometweets.txt")
    val lines = sc.textFile("hdfs://192.168.1.13:8020/user/stratio/resources/sometweets.txt")
    //val lines = sc.textFile("hdfs://192.168.1.13:8020/user/stratio/resources/tinytweets.txt")

    println("What do you mean?")
    println(lines.first())
    val sentimentMaps = lines.filter(_.length>0).mapPartitions(extractSentiment)



    //val sentimentMaps = lines.map(line=>Map("line"-> line, "sentimenter"->sentimenter.classify(line)))
    for (sentimentMap <- sentimentMaps.collect()) {
      println(sentimentMap)
    }
    val timeend = System.currentTimeMillis()
    println("Done in "+ (timeend -timestart) + " ms")

  }


}
