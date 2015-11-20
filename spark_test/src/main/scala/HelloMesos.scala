import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

/**
 * Created by cnavarro on 18/11/15.
 */
object HelloMesos {
  def main (args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("mesos://136.243.53.83:5050")
      //.setMaster("local[4]")
      .setAppName("My app")
      //.set("spark.executor.uri", "http://mixedadmin/repository/spark-1.5.0-bin-hadoop2.4.tgz")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[Double], classOf[Int]))
    val sc = new SparkContext(conf)
    val a : RDD[Int] = sc.parallelize(List(1,2,3,4))
    Thread.sleep(10000)
    val b = a.map(_+1).collect
    println(b.mkString(", "))
    implicit val formats = Serialization.formats(NoTypeHints)
    println(write(b))
  }

}
