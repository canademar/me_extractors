import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

object OrchestratorExample {

  def main (args: Array[String]): Unit = {

    val mods = Array("modA", "modC")

    val sparkConf = new SparkConf(true).setAppName("demoBRM").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val mydata = sc.parallelize(Array("json1", "json2", "json3", "json4", "json5", "json6", "json7", "json8", "json9", "json10"))

    val funcArray = mods.map(findMixEmModule)

    val dummyFunc: (RDD[String] => RDD[String]) = {x => x}

    val compFunc = funcArray.foldLeft(dummyFunc)(_.compose(_))

    val resultJSON = compFunc(mydata)

    resultJSON.foreach(println(_))

  }

  //def composeFuncs(func1: RDD[String] => RDD[String], func2: RDD[String] => RDD[String]): RDD[String] => RDD[String] = func1 _ compose func2

  def findMixEmModule(mod: String): RDD[String] => RDD[String] = {

    mod match {
      case "modA" => runningModA

      case "modB" => runningModB

      case "modC" => runningModC

      case "modX" => runningModX

      case _ => otherwiseMod
    }

  }

  val runningModX: RDD[String] => RDD[String] = _.map({_.concat(" string added by modX ")})

  val runningModA: RDD[String] => RDD[String] = _.map({_.concat(" string added by modA ")})

  val runningModB: RDD[String] => RDD[String] = _.map({_.concat(" string added by modB ")})

  val runningModC: RDD[String] => RDD[String] = _.map({_.concat(" string added by modC ")})

  val runningModD: RDD[String] => RDD[String] = _.map({_.concat(" string added by modD ")})

  val otherwiseMod: RDD[String] => RDD[String] = _.map({_.concat(" string added by otherwise ")})


  /*


  def runningModA(data: RDD[String]): RDD[String] = {
    data.map(x=>x.concat(" string added by modA "))
  }

  def runningModB(data: RDD[String]): RDD[String] = {
    data.map(x=>x.concat(" string added by modB "))
  }

  def runningModC(data: RDD[String]): RDD[String] = {
    data.map(x=>x.concat(" string added by modC "))
  }

  def otherwiseMethod(data: RDD[String]): RDD[String] = {
    data.map(x => x.concat(" string added by the otherwise method"))
  }

  */


}
