import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.sksamuel.elastic4s.ElasticDsl.{create, index, _}
import com.sksamuel.elastic4s.mappings.FieldType.{IntegerType, StringType}
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.elasticsearch.common.settings.ImmutableSettings

import scala.util.parsing.json.JSON
import scalaj.http._

object DWNewsClient {

  // Global variables related to the query

  // brand = spaces+separated+by+a+plus+sign
  val keyword = "real+madrid"
  // langID = 28 (Spanish), 2 (English)
  val langID = "2"
  // date (format) = yyyy-mm-dd
  val lowerLimitDate = "2000-01-01"
  val upperLimitDate = "2015-12-31"
  val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")

  // Global variables related to ES storage

  val ip_host = "136.243.53.82"
  // val ip_host = "localhost"
  val cluster_name = "elasticsearch"
  // val cluster_name = "mixedem_elasticsearch"
  // Name of the index
  val es_index = "dw_news"

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

    // From the JSON object, the field items, which is given by a list of elements (Map), is retained
    val items = json match {
      case Some(m: Map[String, Any]) => m.get("items") match {
        case Some(i: List[Map[String, Any]]) => i
      }
    }

    // For each item, the field reference is extracted. This field contains a set of four elements: id, type, name
    // and url
    val data = items.map(x => x.get("reference") match {
      case Some(r: Map[String, Any]) => r
    })

    return data

  }

  def insertJSONData(inputData: Map[String, Any]): Unit = {
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
      client.execute {
        create index es_index mappings (
          es_type as(
            "dw_id" typed IntegerType,
            "languageId" typed IntegerType,
            "name" typed StringType,
            "url" typed StringType,
            "text" typed StringType,
            "keyword" typed StringType
            )
          )
      }.await
    }

    // Extracting the value of the fields from the input data
    val f_id = inputData.get("id") match {
      case Some(d: Any) => d
    }

    val f_langId = langID // The ID of the language is taken from the intial values of the query

    val f_name = inputData.get("name") match {
      case Some(d: Any) => d
    }

    val f_url = inputData.get("url") match {
      case Some(d: Any) => d
    }
    
    val f_keyword = keyword.replace("+","")

    // From the url field, the text is extracted
    val response: HttpResponse[String] = Http(f_url.toString).asString
    var f_text = ""

    if(response.body.contains("404 Not Found") == false) {
      val json = JSON.parseFull(response.body)

      f_text = json match {
        case Some(m: Map[String, Any]) => m.get("text") match {
          case Some(i: Any) => i.toString
        }
      }
    }

    // Once every field has been obtained, it is indexed in ES
    client.execute {
      index into es_index / es_type fields(
        "dw_id" -> f_id,
        "languageID" -> f_langId,
        "name" -> f_name,
        "url" -> f_url,
        "text" -> f_text,
        "keyword" -> f_keyword
        )
    }.await

    client.close()

  }

  def main (args: Array[String]) {

    var z = true
    var dates = Array("", lowerLimitDate)

    // The loop is running until the time period of the query is within the limits specified by lowerLimitDate and
    // upperLimitDate
    while(z) {
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
      if(response.body.contains("404 Not Found") == false) {
        processJSON(response.body).foreach(x=> insertJSONData(x))
      }

    } // end while

  }

}
