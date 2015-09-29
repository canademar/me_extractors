import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.sksamuel.elastic4s.ElasticDsl.{create, index, _}
import com.sksamuel.elastic4s.mappings.FieldType.{DateType, IntegerType, StringType}
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.sort.SortOrder

import scala.util.parsing.json.JSON
import scalaj.http._

object DWNewsClient {

  // Global variables related to the query

  // brand = spaces+separated+by+a+plus+sign
  val keywords = List("real+madrid","BBVA", "Repsol")
  // langID = 28 (Spanish), 2 (English)
  //val langIDs = List("2","28")
  val langIDs = List("2")
  // date (format) = yyyy-mm-dd
  val lowerLimitDate = "2012-07-01"
  val upperLimitDate = "2015-12-31"
  val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val DWDateFormatter =  new java.text.SimpleDateFormat("yyyy-MM-dd'T'mm:hh:ss.000'Z'")

  // Global variables related to ES storage

  val ip_host = "136.243.53.82"
  // val ip_host = "localhost"
  val cluster_name = "elasticsearch"
  // val cluster_name = "mixedem_elasticsearch"
  // Name of the index
  val es_index = "dw_news"

  val langMap = Map(28 -> "es", 2 -> "en")

  var pageWithResultsCount = 0

  def getLimitDates(currentDates: Array[String]): Array[String] = {

    val fCurrentDate = formatter.parse(currentDates(1))

    val oneMonthMillis = TimeUnit.DAYS.toMillis(30)

    val newDateMillis = fCurrentDate.getTime + oneMonthMillis

    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(fCurrentDate.getTime + oneMonthMillis)

    var d:Array[String] = new Array[String](2)

    d(0) = currentDates(1)
    d(1) = formatter.format(calendar.getTime())

    println("from: " + d(0) + " to: " + d(1))

    return(d)

  }

  def processJSON(jstr: String): List[Map[String, Any]] = {
    // The field items contains the elements found in the website of DW containing the specified keywords.

    // The response, which is obtained as a string, is initially parsed as a JSON object
    val json = JSON.parseFull(jstr)


    val resultCount = json match {
      case Some(m: Map[String, Any]) => {
        m.get("resultCount") match {
          case Some(i: Any) => i.toString.toDouble
        }
      }
      case _ => throw new Exception("Error parsing array")
    }

    var data = List[Map[String, Any]]()

    if(resultCount!=0){
      pageWithResultsCount += 1


      // From the JSON object, the field items, which is given by a list of elements (Map), is retained
      val items = json match {
        case Some(m: Map[String, Any]) =>
          m.get("items") match {
            case Some(i: List[Map[String, Any]]) => i
          }
      }

      // For each item, the field reference is extracted. This field contains a set of four elements: id, type, name
      // and url
        data = items.map(x => x.get("reference") match {
        case Some(r: Map[String, Any]) => r
      })

    }
    println("Pages with results:"+pageWithResultsCount)

    data
  }

  def insertJSONData(keyword: String, inputData: Map[String, Any]): Unit = {
    // Once the obtained response has been processed, the resulting JSON object is stored in the ES database

    // Getting a client for ES
    val uri = ElasticsearchClientUri("elasticsearch://" + ip_host + ":9300")
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build()
    val client = ElasticClient.remote(settings, uri)

    // The type of the documents to be indexed
    val es_type = inputData.get("type") match {
      case Some(d: Any) => d.toString
    }

    // Create the index if it does not exist
    if (client.execute{index exists es_index}.await.isExists == false) {
      println("Index did not exist")
      client.execute {
        create index es_index mappings (
          es_type as(
            "dw_id" typed IntegerType,
            "lang" typed StringType,
            "name" typed StringType,
            "url" typed StringType,
            "text" typed StringType,
            "keyword" typed StringType,
            "project" typed StringType,
            "created_at" typed DateType
            )
          )
      }.await
    }else{
      println("Index exists")
    }

    // Extracting the value of the fields from the input data
    val f_id = inputData.get("id") match {
      case Some(d: Any) => "dw_"+ d.toString
    }


    val f_name = inputData.get("name") match {
      case Some(d: Any) => d
    }

    val f_url = inputData.get("url") match {
      case Some(d: Any) => d
    }
    println("Url: "+ f_url)
    
    val f_keyword = keyword.replace("+","_")

    // From the url field, the text is extracted
    val response: HttpResponse[String] = Http(f_url.toString).asString
    var f_text = ""
    var f_displayDate = Calendar.getInstance().getTime()
    var f_lang = ""

    if(response.isNotError) {
      val json = JSON.parseFull(response.body)



      f_text = json match {
        case Some(m: Map[String, Any]) => m.get("text") match {
          case Some(i: Any) => i.toString
        }
      }
      f_displayDate = json match {
        case Some(m: Map[String, Any]) => m.get("displayDate") match {
          case Some(i: Any) => DWDateFormatter.parse(i.toString)
        }
      }

      val langID = json match{
        case Some(m: Map[String, Any]) => m.get("languageId") match {
          case Some(d: Any) =>  d.toString.toFloat.toInt
        }
      }
      println("LangID " + langID )

      f_lang = langMap.get(langID) match {
        case Some(s: String)=> s
      }// The ID of the language is taken from the intial values of the query


    }





    // Once every field has been obtained, it is indexed in ES
    client.execute {
      index into es_index / es_type id f_id fields(
        "id" -> f_id,
        "lang" -> f_lang,
        "name" -> f_name,
        "url" -> f_url,
        "text" -> f_text,
        "keyword" -> f_keyword,
        "project" -> f_keyword,
        "created_at" -> f_displayDate
        )
    }.await

    client.close()

  }

  def getLowerLimitDate(): String = {

    // Getting a client for ES
    val uri = ElasticsearchClientUri("elasticsearch://" + ip_host + ":9300")
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build()
    val client = ElasticClient.remote(settings, uri)

    client.execute{search in "dw_news" query(
      "*:*"
      ) sort( field sort "created_at" order(SortOrder.DESC))

     }


    lowerLimitDate
  }

  def main (args: Array[String]) {

    var z = true
    var dates = Array("", lowerLimitDate)

    for(keyword <- keywords) {
      println("Querying for "+ keyword)
      for(langID <- langIDs){
        println("In language: " + langID)
      // The loop is running until the time period of the query is within the limits specified by lowerLimitDate and
      // upperLimitDate

        while (z) {
          // Getting the interval time of the query
          dates = getLimitDates(dates)
          // Comparing the upper limit of the interval with the predefined limit in upperLimitDate
          if (formatter.parse(dates(1)).getTime > formatter.parse(upperLimitDate).getTime) {
            z = false
          }

          // The query for the DW service is defined
          val dw_query = "terms=" + keyword + "&languageId=" + langID + "&contentTypes=Article,Video&startDate=" +
            dates(0) + "&endDate=" + dates(1) + "&sortByDate=true&pageIndex=1&asTeaser=false"
          // The REST service is queried and the response (JSON format) is obtained
          val response: HttpResponse[String] = Http("http://www.dw.com/api/search/global?" + dw_query).asString

          // The response in JSON format is processed
          if (response.isNotError) {
            processJSON(response.body).foreach(x => insertJSONData(keyword, x))
          }
          Thread.sleep(500)
        }


      } // end while
    }
    println("Finished")
  }

}
