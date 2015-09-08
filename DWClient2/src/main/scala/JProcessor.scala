import scala.util.parsing.json.JSON

object JProcessor {

  val myjstr = """ {  "lang": 10,
                      "pagInfo": "http://placehold.it/32x32",
                      "items": [
                        {
                          "name": "Ila Rich",
                          "reference": {
                              "id": 88,
                              "text": "text of the item",
                              "url": "url of the item"
                          }
                        },
                        {
                          "name": "Marissa Forbes",
                          "reference": {
                              "id": 475,
                              "text": "text of the item",
                              "url": "url of the item"
                          }
                        },
                        {
                          "name": "Sasha Buchanan",
                          "reference": {
                              "id": 171,
                              "text": "text of the item",
                              "url": "url of the item"
                          }
                        }
                      ]
                    } """


  def processJSON(jstr: String): List[Map[String, Any]] = {

    val json = JSON.parseFull(jstr)

    val items = json match {
      case Some(m: Map[String, Any]) => m.get("items") match {
        case Some(i: List[Map[String, Any]]) => i
      }
    }

    val data = items.map(x => x.get("reference") match {
      case Some(r: Map[String, Any]) => r
    })

/*
    val jsonMap:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]

    val items = jsonMap.get("items").get.asInstanceOf[List[Any]]

    val data = items.map(x => x.asInstanceOf[Map[String, Any]].get("reference").asInstanceOf[Map[String, Any]])
    */

    return data

  }

  def main (args: Array[String]) {

    val postj = processJSON(myjstr)

    postj.head.keySet.foreach(x=>println(x))

    postj.foreach(x=> x.get("id") match {
      case Some(d: Any) => println("\t\t" + d + "\n\n")
    })

  }



}
