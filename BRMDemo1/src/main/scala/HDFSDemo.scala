import java.io.{File, PrintWriter}
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

object HDFSDemo {

  implicit val formats = Serialization.formats(NoTypeHints)

  val nn1 = "192.168.1.13"

  def main (args: Array[String]) {

    val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("mesos://192.168.1.12:5050") //setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //val inputs = sc.textFile("hdfs://"+nn1+":8020/user/stratio/data/projects/BBVA/2016-01-14/twitter/2016-01-14_01-03-06_1")
    val inputs = sc.textFile("hdfs://"+nn1+":8020/user/stratio/data/projects/BBVA/2016-01-14/twitter/")

    println(inputs.first())
    println(inputs.count())





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
