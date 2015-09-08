import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType.{IntegerType, StringType}
import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient, StopAnalyzer}
import org.elasticsearch.common.settings.ImmutableSettings


object ExampleRemoteClient {

  def main (args: Array[String]) {

    //val client = ElasticClient.local
    val uri = ElasticsearchClientUri("elasticsearch://136.243.53.82:9300")
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build()
    val client = ElasticClient.remote(settings, uri)

    //val client = ElasticClient.remote("136.243.53.82", 9300)

    client.execute {
      create index "places" mappings (
        "cities" as(
          "id" typed IntegerType,
          "name" typed StringType boost 4,
          "content" typed StringType analyzer StopAnalyzer
          )
        )
    }.await

    client.execute {
      index into "places/cities" fields(
        "name" -> "London",
        "content" -> "myContent"
        )
    }.await

  }


}
