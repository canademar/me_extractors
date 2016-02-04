import java.text.Normalizer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object SparkOrchestrator {

  def main (args: Array[String]) {

    // Pipeline configuration
    val mods = Array("concept_es", "topic_es", "sent_en")

    // Spark configuration and context
    val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("mesos://192.168.1.12:5050")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // Loading data


    val addData = sc.parallelize(Array("{\"text\": \"I hate western movies\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is really nasty. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}"))



    val initData = sc.textFile("/user/stratio/data/projects/2/2016-01-27/twitter/2016-01-27_12-01-261")

    val data = initData.union(addData)

    // The NOT filter is initially applied tot he data
    val mydata = NotsFilter.filterText(data)

    // The name of the modules to be applied are stored in an array
    val funcArray = mods.map(findMixEmModule)

    // Getting the function that results from the composition of the selected modules/functions
    val dummyFunc: (RDD[String] => RDD[String]) = {x => x}
    val compFunc = funcArray.foldLeft(dummyFunc)(_.compose(_))

    // Data are processed by the selected modules (composed function)
    val resultJSON = compFunc(mydata)

    resultJSON.foreach(println(_))

    println(resultJSON.first())


    val resultEn = resultJSON.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).filter(x=> x.getOrElse("lang","").asInstanceOf[String]=="en")


    if (resultEn.count()>0)
      println(resultEn.first())
    else
      println("No data in english found")


  }

  def findMixEmModule(mod: String): RDD[String] => RDD[String] = {

    mod match {
      case "topic_es" => topicextractor_spanish

      case "concept_es" => conceptextractor_spanish

      case "sent_en" => sentimentextractor_english
    }

  }

  val topicextractor_spanish: RDD[String] => RDD[String] = (x: RDD[String]) => {

    val sc = x.sparkContext

    val tax = JSON.parseFull(sc.textFile("/user/stratio/repository/example_taxonomy.json").collect().foldLeft("")(_.concat(_))).get.asInstanceOf[Map[String,List[String]]]
    //val tax = JSON.parseFull(sc.textFile("/home/jvmarcos/Escritorio/example_taxonomy.json").collect().foldLeft("")(_.concat(_))).get.asInstanceOf[Map[String,List[String]]]

    val mySparkTopicExtractor = new SparkTopicExtractor(tax)

    mySparkTopicExtractor.extractTopicsFromRDD(x)

  }

  val conceptextractor_spanish: RDD[String] => RDD[String] = (x: RDD[String]) => {

    val sc = x.sparkContext

    val tax = parseTaxonomy(sc, "/user/stratio/repository/pagelinks_all.tsv")
    //val tax = parseTaxonomy(sc, "/home/jvmarcos/Escritorio/pagelinks_all.tsv")

    val conceptExtractor = new SparkConceptExtractor(tax, 400, 10)

    conceptExtractor.extractConceptsFromRDD(x)

  }

  val sentimentextractor_english: RDD[String] => RDD[String] = {
    SparkSentiment.extractSentimentFromRDD(_)
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
