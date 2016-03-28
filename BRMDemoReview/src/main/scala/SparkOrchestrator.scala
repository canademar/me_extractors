import java.text.Normalizer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object SparkOrchestrator {
  //val confFilePath = "/home/cnavarro/workspace/mixedemotions/me_extractors/BRMDemo_MarchMeeting/src/main/resources/cnavarro.conf"
  val confFilePath = "/home/cnavarro/me_extractors/BRMDemo_MarchMeeting/src/main/resources/production.conf"

  final val configurationMap : Map[String, String] = {
    Map("languages" -> "es,en",
      "modules" -> "sent_en, topic_es, entities_en",
      "entities_en.conf_path" -> "/var/data/resources/nuig_entity_linking/ie.nuig.me.nel.properties" ,
      "topic_es.taxonomy_path" -> "hdfs://mixedemotions/user/stratio/repository/example_taxonomy.json",
      "concept_es.taxonomy_path" -> "hdfs://mixedemotions/user/stratio/repository/pagelinks_all.tsv",
      "sent_en.resources_folder" -> "/var/data/resources/nuig_sentiment/",
      "elasticsearch.ip" -> "192.168.1.12",
      "elasticsearch.port" -> "9300",
      "elasticsearch.clusterName" -> "Mixedemotions Elasticsearch",
      "elasticsearch.indexName" -> "myanalyzed",
      "entities_en.conf_path" -> "/var/data/resources/nuig_entity_linking/ie.nuig.me.nel.properties"
    )
  }

  def main (args: Array[String]) {

    // Pipeline configuration
    val configuration = configurationMap
    val mods : Array[String] = configuration("modules").split(",").reverse
    mods.foreach(mod=>println("\n\n--------mod: " + mod + " -----------\n\n"))
    //val mods = Array("persistor")


    // Spark configuration and context
    //val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("mesos://192.168.1.12:5050")//.setMaster("local[*]")
    //val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("local[*]")
    val sparkConf = new SparkConf(true).setAppName("demoBRM")
    val sc = new SparkContext(sparkConf)


    // Loading data

    println("\nLoading data  -------\n")

    val addData = sc.parallelize(Array("{\"text\": \"I hate western movies with John Wayne\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Really nice car\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is really nasty. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"The new Star Wars film is awesome, but maybe it is just for fans. You will not enjoy it anyway\", \"nots\": [\"hola\"], \"lang\": \"en\"}",
      "{ \"text\": \"Hola ke ace?\", \"nots\": [\"hola\"], \"lang\": \"es\"}",
      "{ \"text\": \"You have a horrible car\", \"nots\": [\"hola\", \"adios\"], \"lang\": \"en\"}",
      "{ \"text\": \"You have a really nice car\", \"nots\": [\"hola\", \"adios\"], \"lang\": \"en\"}",
      "{ \"text\": \"Wow!!! It's an amazing car! I like it!\", \"nots\": [\"hola\", \"adios\"], \"lang\": \"en\"}",
      "{ \"text\": \"don't like comedies as they don't make me laugh\", \"nots\": [\"hola\", \"adios\"], \"lang\": \"en\"}",
      "{ \"text\": \"I support Swansea. It has been the most important welsh club in history\", \"nots\": [\"hola\", \"adios\"]," +
        " \"lang\": \"en\"}",
      "{ \"text\": \"I prefer reading biographies. They are considerably funnier than conventional novels\", " +
        "\"nots\": [\"hola\", \"adios\"], \"lang\": \"en\"}",
      "{ \"text\": \"Please, pay attention to the next film by Tarantino. It's absolutely fantastic!\", \"nots\": " +
        "[\"hola\", \"adios\"], \"lang\": \"en\"}",
      "{ \"text\": \"La nueva de Star Wars está muy bien. Me encantó el robot pelota.\", \"nots\": [\"hola\"], \"lang\": \"es\"}"))




    //val initData = sc.textFile("/home/cnavarro/workspace/mixedemotions/data/2016-02-07_02-18-35_1")
    //val initData = sc.textFile("/home/cnavarro/workspace/mixedemotions/data/2016-02-07/BBVA/2016-02-07_19-21-27_1")
    //val initData = sc.textFile("hdfs:///user/stratio/data/projects/1/2016-02-01/twitter/2016-02-01_21-31-20_1")
    //val initData = sc.textFile("hdfs:///user/stratio/data/projects/1/2016-02-01/twitter/")
    val inputPath = args(0)
    //val inputPath = "hdfs:///user/stratio/data/projects/1/2016-03-16/twitter/"
    //val inputPath = "hdfs:///user/stratio/data/projects/2/2016-01-27/twitter/"

    println(s"\nGoing to folder... $inputPath\n")
    //val initData = sc.textFile(inputPath)
    val folderData = sc.wholeTextFiles(inputPath)
    val filesData = folderData.map(pair=> pair._2)
    val initData = filesData.flatMap(_.split("\n"))
    //val data = initData.union(addData)
    val data = initData.union(addData)

    println("\nTotal number of raw data to process: " + data.count() + "\n")

    // The NOT filter is initially applied tot he data
    val mydata = NotsFilter.filterText(data)
    println("\nNumber of items after initial filtering: " + mydata.count() + "\n")

    // The name of the modules to be applied are stored in an array
    val funcArray = mods.map(findMixEmModule)

    // Getting the function that results from the composition of the selected modules/functions
    val dummyFunc: (RDD[String] => RDD[String]) = {x => x}
    val compFunc = funcArray.foldLeft(dummyFunc)(_.compose(_))

    // Data are processed by the selected modules (composed function)
    val resultJSON = compFunc(mydata).cache()
    val numResultJSON = resultJSON.count()

    println("\nNumber of items after processing (resultJSON): " + numResultJSON + "\n")

    //val collected = resultJSON.collect
    //println("Reactivate persistence")
    //persistWithoutSpark(collected)
    //println("Num results: " + collected.length.toString)

    val resultEn = resultJSON.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String,Any]())).filter(x=> x.getOrElse("lang","").asInstanceOf[String]=="en")
    val numResultEn = resultEn.count()

    println("\nNumber of items (in english) after processing (resultEn): " + numResultEn + "\n")

    if (numResultEn>10){
      val rEn = resultEn.take(10)
      rEn.foreach(println(_))
    }
    else{
      println("No data in english found")
    }

  }

  def findMixEmModule(mod: String): RDD[String] => RDD[String] = {

    mod.trim match {
      case "topic_es" => topicextractor_spanish

      case "concept_es" => conceptextractor_spanish

      case "sent_en" => sentimentextractor_english

      case "upm_sent" => upm_sentiment_extractor

      case "entities_en" => entitylinking_english

     // case "persistor" => elasticsearch_persistor

      case "emotions" => emotion_extractor

    }

  }

  val topicextractor_spanish: RDD[String] => RDD[String] = (x: RDD[String]) => {

    println("Extracting topics")

    val sc = x.sparkContext

    //val taxonomyPath = masterConfiguration.getString("conf.topic_es.taxonomy_path")
    val taxonomyPath = configurationMap("topic_es.taxonomy_path")

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

    val tax = parseTaxonomy(sc, taxonomyPath)
    //val tax = parseTaxonomy(sc, "/home/jvmarcos/Escritorio/pagelinks_all.tsv")

    val conceptExtractor = new SparkConceptExtractor(tax, 400, 10)

    val result = conceptExtractor.extractConceptsFromRDD(x)

    println("Finished concept extractor")

    result

  }

  /*
  val sentimentextractor_english: RDD[String] => RDD[String] = {
    println("Sentiment extractor")
    //val resourcesFolder = masterConfiguration.getString("conf.sent_en.resources_folder")
    val resourcesFolder = "/var/data/resources/nuig_sentiment/"
    println("******Got resources folder " + resourcesFolder)
    SparkSentiment.extractSentimentFromRDD(_, resourcesFolder)
  }
  */

  val sentimentextractor_english: RDD[String] => RDD[String] = (x:RDD[String]) => {
    println("Going to sentiment extractor")
    //val resourcesFolder = masterConfiguration.getString("conf.sent_en.resources_folder")
    val resourcesFolder = "/var/data/resources/nuig_sentiment/"
    val result = SparkSentiment.extractSentimentFromRDD(x, resourcesFolder)
    println("Sentiment extractor finished")
    result
  }


/*
  val entitylinking_english: RDD[String] => RDD[String] = {
    println("Entity linking")
    val confPath = configurationMap("entities_en.conf_path")
    NUIGEntityLinkingExtractor.extractEntityLinkingFromRDD(_, confPath)
  }
  */

  val entitylinking_english: RDD[String] => RDD[String] = (x:RDD[String]) => {
    println("Entity linking")
    val confPath = configurationMap("entities_en.conf_path")
    val result = NUIGEntityLinkingExtractor.extractEntityLinkingFromRDD(x, confPath)
    println("Entity linking finished")
    result
  }


  val emotion_extractor: RDD[String] => RDD[String] = {
    UPMEmotionAnalysisService.process(_)
  }

  val upm_sentiment_extractor: RDD[String] => RDD[String] = {
    UPMSentimentAnalysisService.process(_)
  }

  /*val elasticsearch_persistor: RDD[String] => RDD[String] = {
    /*val esIP = "mixednode2"
    val esPort = 9300
    val esClusterName = "Mixedemotions Elasticsearch"
    */
    val esIP = configurationMap("elasticsearch.ip")
    val esPort = configurationMap("elasticsearch.port").toInt
    val esClusterName = configurationMap("elasticsearch.clusterName")
    val indexName = configurationMap("elasticsearch.indexName")

    println("Going to persist")


    ElasticsearchPersistor.persistTweetsFromRDDmp(_, esIP, esPort , esClusterName, indexName)


  }*/

  def persistWithoutSpark(tweets : Array[String]): Unit =  {
    /*val esIP = "mixednode2"
    val esPort = 9300
    val esClusterName = "Mixedemotions Elasticsearch"
    */
    val esIP = configurationMap("elasticsearch.ip")
    val esPort = configurationMap("elasticsearch.port").toInt
    val esClusterName = configurationMap("elasticsearch.clusterName")
    val indexName = configurationMap("elasticsearch.indexName")

    println("Going to persist")


    ElasticsearchPersistor.persistTweetsWithoutSpark(tweets.toList, esIP, esPort , esClusterName, indexName)


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
