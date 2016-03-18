import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON


object NotsFilter {

  def filterText(input: RDD[String]): RDD[String] = input.map(x => JSON.parseFull(x)
    .asInstanceOf[Some[Map[String, Any]]].getOrElse(Map[String, Any]())).filter(x => {
    val nots = x.get("nots").asInstanceOf[Some[List[String]]].getOrElse(List[String]()).map(x=>x.toLowerCase())
    val text = x.get("text").asInstanceOf[Some[String]].getOrElse("")
    val resultArray = nots.map(x => {
      !(text.toLowerCase.contains(x))
    })
    val result = resultArray.foldLeft(true)(_ & _)
    result
  }).map(x => {

    implicit val formats = Serialization.formats(NoTypeHints)
    write(x)

  })

  def filterTextNoJSONOut(input: RDD[String]): RDD[Map[String, Any]] = input.map(x => JSON.parseFull(x)
    .asInstanceOf[Some[Map[String, Any]]].getOrElse(Map[String, Any]())).filter(x => {
    val nots = x.get("nots").asInstanceOf[Some[List[String]]].getOrElse(List[String]()).map(x=>x.toLowerCase())
    val text = x.get("text").asInstanceOf[Some[String]].getOrElse("")
    val resultArray = nots.map(x => {
      !(text.toLowerCase.contains(x))
    })
    val result = resultArray.foldLeft(true)(_ & _)
    result
  })


}
