import edu.insight.unlp.nn.example.TwitterSentiMain
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON



/**
 * Created by cnavarro on 24/11/15.
 */
object SparkSentiment {
  val resourcesPath: String = "/var/data/resources/nuig_sentiment/"
  //val resourcesPath: String = "/home/jvmarcos/Proyectos/MixedEmotions/Development/BRMDemo_MarchMeeting/src/main/resources/"
  val glovePath: String = resourcesPath + "embeddings/glove.twitter.27B.50d.txt"
  val dataPath: String = resourcesPath + "data/twitterSemEval2013.tsv"
  val modelPath: String = resourcesPath + "model/learntSentiTwitter.model"

  def extractSentiment(lines: Iterator[String]): Iterator[Map[String,Any]] = {
    val timestart = System.currentTimeMillis()
    val sentimenter: TwitterSentiMain = new TwitterSentiMain
    println("modelPath = " + modelPath)
    println("glovePath = " + modelPath)
    println("dataPath = " + dataPath)
    sentimenter.loadModel(modelPath, glovePath, dataPath)
    val timeend = System.currentTimeMillis()
    println("Time to initialize model: " + (timeend - timestart))
    //    var result = new mutable.MutableList[Map[String, Any]]()
    (lines).map { case line => Map("line" -> line, "sentiment" -> sentimenter.classify(line)) }


  }

  def extractSentimentFromMap(lines: Iterator[scala.collection.mutable.Map[String,Any]]) : Iterator[scala.collection.mutable
  .Map[String,Any]]= {

    val sentimenter: TwitterSentiMain = new TwitterSentiMain
    sentimenter.loadModel(modelPath, glovePath, dataPath)

    for(line<-lines) yield {
      val text = line.getOrElse("text","").asInstanceOf[String]
      val lang = line.getOrElse("lang","").asInstanceOf[String]
      if(text.length>0 && lang.equals("en")){
        //line + (("sentiment", sentimenter.classify(text)))
        line += ("sentiment"-> sentimenter.classify(text))
        //Map("_source"->(line.getOrElse("_source", Map[String,String]()).asInstanceOf[Map[String,String]] + (("sentiment", sentimenter.classify(text)))))
      }else{
        line += ("sentiment"-> "No sentiment found")
      }
    }


  }
  

  def extractSentimentFromRDD(input: RDD[String]): RDD[String] = {

    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = temp.mapPartitions(item =>extractSentimentFromMap(item))

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

  

}