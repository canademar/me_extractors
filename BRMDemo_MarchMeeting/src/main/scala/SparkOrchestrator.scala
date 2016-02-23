import java.io.File
import java.text.Normalizer

import com.typesafe.config.{ConfigFactory,Config}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object SparkOrchestrator {
  //val confFilePath = "/home/cnavarro/workspace/mixedemotions/me_extractors/BRMDemo_MarchMeeting/src/main/resources/cnavarro.conf"
  val confFilePath = "/home/cnavarro/me_extractors/BRMDemo_MarchMeeting/src/main/resources/production.conf"

  /*val masterConfiguration : Config = {
    val parsedConf = ConfigFactory.parseFile(new File(confFilePath))
    ConfigFactory.load(parsedConf)
    /*val conf = ConfigFactory.load(parsedConf)
    Map("entities_en" -> conf.getString("conf.entities_en"),
        "topic_es" -> conf.getString("conf.topic_es"),
        "concept_es" -> conf.getString("conf.concept_es"),
        "sent_en" ->conf.getString("conf.sent_en"))*/


  }*/

  /*final val configurationMap : Map[String, String] = {
    val confFile = new File(confFilePath)
    val parsedConf = ConfigFactory.parseFile(confFile)
    val conf = ConfigFactory.load(parsedConf)
    Map("languages" -> conf.getStringList("languages").toArray.mkString(","),
        "modules" -> conf.getStringList("modules").toArray.mkString(","),
        "entities_en.conf_path" -> conf.getString("entities_en.conf_path"),
        "topic_es.taxonomy_path" -> conf.getString("topic_es.taxonomy_path"),
        "concept_es.taxonomy_path" -> conf.getString("concept_es.taxonomy_path"),
        "sent_en.resources_folder" ->conf.getString("sent_en.resources_folder"),
        "elasticsearch.ip" ->conf.getString( "elasticsearch.ip"),
        "elasticsearch.port" ->conf.getString( "elasticsearch.port"),
        "elasticsearch.clusterName" ->conf.getString( "elasticsearch.clusterName"),
        "elasticsearch.indexName" ->conf.getString( "elasticsearch.indexName")
    )
  }*/

  final val configurationMap : Map[String, String] = {
    val confFile = new File(confFilePath)
    val parsedConf = ConfigFactory.parseFile(confFile)
    val conf = ConfigFactory.load(parsedConf)
    Map("languages" -> "es,en",
      //"modules" -> "topic_es,concept_es,sent_en,persistor",
      "modules" -> "concept_es,topic_es,sent_en,persistor",
      "entities_en.conf_path" -> "/var/data/resources/nuig_entity_linking/ie.nuig.me.nel.properties" ,
      "topic_es.taxonomy_path" -> "hdfs:///user/stratio/repository/example_taxonomy.json",
      "concept_es.taxonomy_path" -> "hdfs://192.168.1.12:8020/user/stratio/repository/pagelinks_all.tsv",
      //"concept_es.taxonomy_path" -> "/var/data/resources/pt_concepts",
      "sent_en.resources_folder" -> "/var/data/resources/nuig_sentiment/",
      "elasticsearch.ip" -> "192.168.1.12",
      "elasticsearch.port" -> "9300",
      "elasticsearch.clusterName" -> "Mixedemotions Elasticsearch",
      "elasticsearch.indexName" -> "myanalyzed"
    )
  }

  def main (args: Array[String]) {

    // Pipeline configuration
    //val mods = Array("concept_es", "topic_es", "sent_en", "entities_en")
    val configuration = configurationMap
    //val mods : Array[String] = masterConfiguration.getStringList("modules").toArray(Array())
    val mods : Array[String] = configuration("modules").split(",").reverse
    //val mods = Array("persistor")


    // Spark configuration and context
    //val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("mesos://192.168.1.12:5050")//.setMaster("local[*]")
    //val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("local[*]")
    val sparkConf = new SparkConf(true).setAppName("demoBRM")
    val sc = new SparkContext(sparkConf)

    // Loading data

    println("Starting  -------")

    /*val addData = sc.parallelize(Array("{\"text\": \"I hate western movies with John Wayne\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is really nasty. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is awesome, but maybe it is just for fans. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Hola ke ace?\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"La nueva de Star Wars está muy bien. Me encantó el robot pelota.\", \"nots\": [\"hola\"], \"lang\": \"es\"}"))
      */



    //val initData = sc.textFile("/home/cnavarro/workspace/mixedemotions/data/2016-02-07_02-18-35_1")
    //val initData = sc.textFile("/home/cnavarro/workspace/mixedemotions/data/2016-02-07/BBVA/2016-02-07_19-21-27_1")
    //val initData = sc.textFile("hdfs:///user/stratio/data/projects/1/2016-02-01/twitter/2016-02-01_21-31-20_1")
    val initData = sc.textFile("hdfs:///user/stratio/data/projects/1/2016-02-01/twitter/")
    //val data = initData.union(addData)
    val data = initData

    // The NOT filter is initially applied tot he data
    val mydata = NotsFilter.filterText(data)

    println("Filtered data-------------")

    // The name of the modules to be applied are stored in an array
    val funcArray = mods.map(findMixEmModule)

    println("Mapping")

    // Getting the function that results from the composition of the selected modules/functions
    val dummyFunc: (RDD[String] => RDD[String]) = {x => x}
    val compFunc = funcArray.foldLeft(dummyFunc)(_.compose(_))

    // Data are processed by the selected modules (composed function)
    val resultJSON = compFunc(mydata).cache()

    println("Got result json")




    val resultEn = resultJSON.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).filter(x=> x.getOrElse("lang","").asInstanceOf[String]=="en")


    if (resultEn.count()>0){
      println(resultEn.first())
    }
    else{
      println("No data in english found")
    }

    val results = resultJSON.collect()
    println("First------------------------------------")
    println(results.head)
    /*for(result <- resultJSON.collect()){
      print("Result: ")
      println(result)
    }*/


  }

  def findMixEmModule(mod: String): RDD[String] => RDD[String] = {

    mod.trim match {
      case "topic_es" => topicextractor_spanish

      case "concept_es" => conceptextractor_spanish

      case "sent_en" => sentimentextractor_english

     //  case "entities_en" => entitylinking_english
      case "persistor" => elasticsearch_persistor
    }

  }


  val topicextractor_spanish: RDD[String] => RDD[String] = (x: RDD[String]) => {

    println("Extracting topics")

    val sc = x.sparkContext

    //val taxonomyPath = masterConfiguration.getString("conf.topic_es.taxonomy_path")
    val taxonomyPath = configurationMap("topic_es.taxonomy_path")

    println("Conf ok. Going. Topci_extractor taxonomy path:" + taxonomyPath)

    val tax = JSON.parseFull(sc.textFile(taxonomyPath).collect().foldLeft("")(_.concat(_))).get.asInstanceOf[Map[String,List[String]]]
    //val tax = JSON.parseFull(sc.textFile("/home/jvmarcos/Escritorio/example_taxonomy.json").collect().foldLeft("")(_.concat(_))).get.asInstanceOf[Map[String,List[String]]]

    val mySparkTopicExtractor = new SparkTopicExtractor(tax)

    val result = mySparkTopicExtractor.extractTopicsFromRDD(x)

    println("Finished topic extractor")

    result

  }

  val conceptextractor_spanish: RDD[String] => RDD[String] = (x: RDD[String]) => {

    println("Extracting concepts")

    val sc = x.sparkContext

    //val taxonomyPath = masterConfiguration.getString("conf.concept_es.taxonomy_path")
    val taxonomyPath = configurationMap("concept_es.taxonomy_path")
    //val taxonomyPath = "hdfs:///user/stratio/repository/pagelinks_all.tsv"

    println("Conf ok. Going. Concept extractor taxonomy path:" + taxonomyPath)

    val tax = parseTaxonomy(sc, taxonomyPath)
    //val tax = parseTaxonomy(sc, "/home/jvmarcos/Escritorio/pagelinks_all.tsv")

    val conceptExtractor = new SparkConceptExtractor(tax, 400, 10)

    val result = conceptExtractor.extractConceptsFromRDD(x)

    println("Finished concepts")

    result

  }

  val sentimentextractor_english: RDD[String] => RDD[String] = {
    println("Going to sentiment extractor")
    //val resourcesFolder = masterConfiguration.getString("conf.sent_en.resources_folder")
    val resourcesFolder = "/var/data/resources/nuig_sentiment/"
    println("******Got resources folder " + resourcesFolder)
    SparkSentiment.extractSentimentFromRDD(_, resourcesFolder)
  }


/*
  val entitylinking_english: RDD[String] => RDD[String] = {
    val confPath = masterConfiguration.getString("conf.entities_en.conf_path")
    println("***************************Conf path " + confPath)
    NUIGEntityLinkingExtractor.extractEntityLinkingFromRDD(_, confPath)
  }
  */

  val elasticsearch_persistor: RDD[String] => RDD[String] = {
    /*val esIP = "mixednode2"
    val esPort = 9300
    val esClusterName = "Mixedemotions Elasticsearch"
    */
    val esIP = configurationMap("elasticsearch.ip")
    val esPort = configurationMap("elasticsearch.port").toInt
    val esClusterName = configurationMap("elasticsearch.clusterName")
    val indexName = configurationMap("elasticsearch.indexName")

    ElasticsearchPersistor.persistTweetsFromRDD(_, esIP, esPort , esClusterName, indexName)


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
