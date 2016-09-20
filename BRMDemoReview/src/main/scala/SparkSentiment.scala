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

  def extractSentiment(lines: Iterator[String], resourcesFolder: String): Iterator[Map[String,Any]] = {
    val glovePath: String = resourcesFolder + "embeddings/glove.twitter.27B.50d.txt"
    val dataPath: String = resourcesFolder + "data/twitterSemEval2013.tsv"
    val modelPath: String = resourcesFolder + "model/learntSentiTwitter.model"

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

  def extractSentimentFromMap(lines: Iterator[scala.collection.mutable.Map[String,Any]], resourcesFolder:String) : Iterator[scala.collection.mutable
  .Map[String,Any]]= {

    val sentimenter: TwitterSentiMain = new TwitterSentiMain
    val glovePath: String = resourcesFolder + "embeddings/glove.twitter.27B.50d.txt"
    val dataPath: String = resourcesFolder + "data/twitterSemEval2013.tsv"
    val modelPath: String = resourcesFolder + "model/learntSentiTwitter.model"

    println("******************glovePath: "+glovePath)
    println("******************dataPath: "+dataPath)
    println("******************modelPath: "+modelPath)

    sentimenter.loadModel(modelPath, glovePath, dataPath)

    for(line<-lines) yield {
      val text = line.getOrElse("text","").asInstanceOf[String]
      val lang = line.getOrElse("lang","").asInstanceOf[String]
      if(text.length>0 && lang.equals("en")){
        //line + (("sentiment", sentimenter.classify(text)))
        val polarity = sentimenter.classify(text).toLowerCase
        line += ("polarity"-> polarity, "sentiment"->getSentimentScore(polarity))
        //Map("_source"->(line.getOrElse("_source", Map[String,String]()).asInstanceOf[Map[String,String]] + (("sentiment", sentimenter.classify(text)))))
      }else{
        line += ("polarity"-> "neutral", "sentiment"->0)
      }
    }


  }

  def getSentimentScore(polarity: String) : Double = {
    if(polarity=="positive"){
      1.0
    }else if(polarity=="negative"){
      -1.0
    }else{
      0
    }
  }

  def extractSentimentFromRDD(input: RDD[String], resourcesFolder: String): RDD[String] = {

    println("\t\tExtracting sentiment")

    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = temp.mapPartitions(item =>extractSentimentFromMap(item, resourcesFolder))

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
