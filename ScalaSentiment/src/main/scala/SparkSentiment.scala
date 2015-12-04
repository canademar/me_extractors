import edu.insight.unlp.nn.example.TwitterSentiMain
import org.apache.spark.{SparkContext, SparkConf}

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
