package legacy

import java.util.Calendar

import org.apache.spark.rdd.RDD

class DummyProcessor extends Serializable {

  def dummyMethodMapPartitions(input: RDD[String]): RDD[List[String]] = {

    val output = input.mapPartitions(x=> {
      var resultList = List[List[String]]()
      while(x.hasNext) {
        val tempList = dummyNLPProcessor(x.next())
        resultList .::= (tempList)
      }
      resultList.iterator
    })

    output

  }

  def dummyMethodMap(input: RDD[String]): RDD[List[String]] = input.map(x=> dummyNLPProcessor(x))


  def dummyNLPProcessor(input: String): List[String] = List[String]("result1", "prefix_" + input + "_suffix",
    "result3", Calendar.getInstance().getTime().toString)

}

object DummyProcessor{
  def main(args: Array[String]) {
    println(Calendar.getInstance().getTime().toString)
  }
}