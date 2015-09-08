import com.sksamuel.elastic4s.ElasticDsl.{create, index, _}
import com.sksamuel.elastic4s.mappings.FieldType.{IntegerType, StringType}
import com.sksamuel.elastic4s.source.DocumentMap
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri, StopAnalyzer}
import org.elasticsearch.common.settings.ImmutableSettings


object ExampleESInserter {

  case class Band(name:String, albums:Seq[String], label:String) extends DocumentMap {
    def map = Map("name" -> name, "albums"->albums.mkString(" "), "label" -> label)
  }

  def main (args: Array[String]) {

    //val client = ElasticClient.local
    val uri = ElasticsearchClientUri("elasticsearch://localhost:9300")
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mixedem_elasticsearch").build()
    val client = ElasticClient.remote(settings, uri)

    if (client.execute{index exists "dw_bbva"}.await.isExists == false) {
      client.execute {
        create index "dw_news" mappings (
          "bbva_news" as(
            "id" typed IntegerType,
            "type" typed StringType,
            "name" typed StringType,
            "url" typed StringType
            )
          )
      }.await
    }

    if (client.execute{index exists "bands"}.await.isExists) {
      println("Index already exists!")
      val num = readLine("Want to delete the index? ")
      if (num == "y") {
        client.execute {deleteIndex("bands")}.await
      } else {
        println("Leaving this here ...")
      }
    } else {
      println("Creating the index!")
      client.execute(create index "bands").await
      client.execute(index into "bands/artists" fields "name"->"coldplay").await
      val resp = client.execute(search in "bands/artists" query "coldplay").await
      println(resp)
    }

    Thread.sleep(5000)

    client.execute {
      create index "places" mappings (
        "cities" as(
          "id" typed IntegerType,
          "name" typed StringType boost 4,
          "content" typed StringType analyzer StopAnalyzer
          )
        )
    }.await

    Thread.sleep(5000)
    client.execute {
      index into "places/cities" id "uk" fields(
        "name" -> "London",
        "content" -> "myContent"
        )
    }.await

    Thread.sleep(5000)

    client.execute { create index "music" }.await

    Thread.sleep(5000)

    //case class Band(name: String, albums: Seq[String], label: String)
    val band = Band("coldplay", Seq("X&Y", "Parachutes"), "Parlophone")

    client.execute {
      // the band object will be implicitly converted into a DocumentSource
      index into "music" / "bands" doc band
    }

    client.execute {
      index into "music/bands" id "uk" fields (
        "name" -> "London",
        "country" -> "United Kingdom",
        "continent" -> "Europe",
        "status" -> "Awesome"
        )
    }.await

    Thread.sleep(5000)







    // we need to wait until the index operation has been flushed by the server.
    // this is an important point - when the index future completes, that doesn't mean that the doc
    // is necessarily searchable. It simply means the server has processed your request and the doc is
    // queued to be flushed to the indexes. Elasticsearch is eventually consistent.
    // For this demo, we'll simply wait for 2 seconds (default refresh interval is 1 second).
    //Thread.sleep(2000)

    // now we can search for the document we indexed earlier
    //val resp = client.execute { search in "bands" / "artists" query "coldplay" }.await
    //println(resp)

  }



}
