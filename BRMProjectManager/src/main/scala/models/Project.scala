package models


import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.parsing.json.JSON

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

/**
 * Created by cnavarro on 1/02/16.
 */
class Project(val id: String, val name: String, val langs: List[String], val synonyms: List[String],
              val nots: List[String], val start: DateTime, val end: DateTime, val period: String) {

  override def toString: String = {
    s"models.Project: { id:$id, name:$name, langs:${langs}, synonyms:${synonyms}, nots:${nots}, period:${period} }"
  }
}

object Project{

  def projectsFromJson(jsonString: String) : List[Project] = {
    val unparsed = JSON.parseFull(jsonString).get.asInstanceOf[List[Map[String,Any]]]

    for(parsedProject: Map[String,Any] <- unparsed){

    }

    parseProjects(unparsed)
  }

  def parseProjects(unparsedProjects: List[Map[String, Any]]) : List[Project] = {
    unparsedProjects match{
      case x::xs => parseProject(x)::parseProjects(xs)
      case Nil => List()

    }
  }

  def parseProject(unparsedProject: Map[String, Any]): Project = {
    val projectId = unparsedProject("id").asInstanceOf[Double].toInt.toString
    val name = unparsedProject("name").asInstanceOf[String]
    val langs = unparsedProject("langs").asInstanceOf[List[String]]
    val synonyms = unparsedProject("synonyms").asInstanceOf[List[String]]
    val nots = unparsedProject("nots").asInstanceOf[List[String]]



    val start = DateTime.parse(unparsedProject("start").asInstanceOf[String], DateTimeFormat.forPattern("yyyyMMdd"))
    val end = DateTime.parse(unparsedProject("end").asInstanceOf[String], DateTimeFormat.forPattern("yyyyMMdd"))
    val period = unparsedProject("period").asInstanceOf[String]

    new Project(projectId, name, langs, synonyms, nots, start, end, period)
  }

  def toJson(projects: List[Project]) : String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    write(projects)
  }

  def main (args: Array[String]) {
    //val jsonText = "[\n    {\n        \"name\": \"Kutxabank\",\n        \"langs\": [\"en\", \"es\"],\n        \"synonyms\": [\"Kutxabank\", \"kutxabank\"],\n        \"nots\": [],\n        \"id\": 1,\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"BBVA\",\n        \"langs\": [\"en\", \"es\"],\n        \"synonyms\": [\"BBVA\", \"banco bilbao vizcaya\", \"bbv argentaria\", \"bbva_esp\"],\n        \"nots\": [\"liga\", \"league\", \"futbol\", \"soccer\", \"match\"],\n        \"id\": 2,\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Caixabank\",\n        \"langs\": [\"en\", \"es\"],\n        \"synonyms\": [\"Caixabank\", \"la caixa\", \"lacaixa\", \"caixabank\", \"cuenta verde de microbank\", \"infocaixa\"],\n        \"nots\": [],\n        \"id\": 3,\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Laboral_Kutxa\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"Laboral Kutxa\",  \"laboralkutxa\",  \"laboral-kutxa\"],\n        \"nots\": [],\n        \"id\": 4,\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"INGdirect\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"INGdirect\",  \"ing-direct\",  \"ing direct\",  \"ingdirectes\"],\n        \"nots\": [],\n        \"id\": 5,\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Leroy_Merlin\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"Leroy Merlin\",  \"leroymerlin\",  \"leroy-merlin\",  \"leroymerlin_es\"],\n        \"nots\": [],\n        \"id\": 6,\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Porcelanosa\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"Porcelanosa\",  \"porcelanosa_es\"],\n        \"nots\": [],\n        \"id\": 7,\n        \"period\": \"12\"\n    }\n]"
    val jsonText = "[\n    {\n        \"name\": \"Kutxabank\",\n        \"langs\": [\"en\", \"es\"],\n        \"synonyms\": [\"Kutxabank\", \"kutxabank\"],\n        \"nots\": [],\n        \"id\": 1,\n        \"start\": \"20160203\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"BBVA\",\n        \"langs\": [\"en\", \"es\"],\n        \"synonyms\": [\"BBVA\", \"banco bilbao vizcaya\", \"bbv argentaria\", \"bbva_esp\"],\n        \"nots\": [\"liga\", \"league\", \"futbol\", \"soccer\", \"match\"],\n        \"id\": 2,\n        \"start\": \"20160101\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Caixabank\",\n        \"langs\": [\"en\", \"es\"],\n        \"synonyms\": [\"Caixabank\", \"la caixa\", \"lacaixa\", \"caixabank\", \"cuenta verde de microbank\", \"infocaixa\"],\n        \"nots\": [],\n        \"id\": 3,\n        \"start\": \"20160101\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Laboral_Kutxa\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"Laboral Kutxa\",  \"laboralkutxa\",  \"laboral-kutxa\"],\n        \"nots\": [],\n        \"id\": 4,\n        \"start\": \"20160101\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"INGdirect\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"INGdirect\",  \"ing-direct\",  \"ing direct\",  \"ingdirectes\"],\n        \"nots\": [],\n        \"id\": 5,\n        \"start\": \"20160101\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Leroy_Merlin\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"Leroy Merlin\",  \"leroymerlin\",  \"leroy-merlin\",  \"leroymerlin_es\"],\n        \"nots\": [],\n        \"id\": 6,\n        \"start\": \"20160101\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    },\n    {\n        \"name\": \"Porcelanosa\",\n        \"langs\": [ \"en\",  \"es\"],\n        \"synonyms\": [ \"Porcelanosa\",  \"porcelanosa_es\"],\n        \"nots\": [],\n        \"id\": 7,\n        \"start\": \"20160101\",\n        \"end\": \"20160501\",\n        \"period\": \"12\"\n    }\n]"
    val projects = projectsFromJson(jsonText)
    for(project <- projects){
      println(project)
    }
    println("To Json")
    println(Project.toJson(projects))
  }

}
