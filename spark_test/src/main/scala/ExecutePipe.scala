import org.apache.spark.{SparkFiles, SparkContext, SparkConf}

/**
 * Created by cnavarro on 15/10/15.
 */
object ExecutePipe {
  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("External Pipe Application")
    val sc = new SparkContext(conf)
    //val textFile = "/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/volkswagen.txt"
    val textFile = "/home/cnavarro/workspace/mixedemotions/spark_test/src/resources/text_to_count.json"
    val lines = sc.textFile(textFile, 3).cache()

    //val distScript = "./src/resources/count_words_fileinput.py"
    //val distScriptName = "count_words_fileinput.py"
    //val distScript = "./src/resources/count_words_wrapper.sh"
    //val distScriptName = "count_words_wrapper.sh"

    val distScript = "./src/resources/count_words_stdin_json.py"
    val distScriptName = "count_words_stdin_json.py"
    sc.addFile(distScript)


    //val counts = lines.pipe(Seq(SparkFiles.get(distScriptName)))
    val counts = lines.pipe(SparkFiles.get(distScriptName))
    counts.collect()
    for(count<-counts){
      println("Found Count: "+count)
    }

    /*val data = List(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    val a = sc.parallelize(1 to 9, 3)
    val a_results = a.pipe("head -n 1").collect
    for(result<- a_results){
      println("result:"+result)
    }*/
  }

}
