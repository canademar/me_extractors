import org.apache.spark.rdd.RDD

import scalaj.http.{HttpResponse, _}

class NetworkAnalysisService {

  // The elements of the original RDD are separately processed. the mapPartitions method is used to optimize performance
  def processViaRestService(rdd: RDD[String]): RDD[String] = rdd.mapPartitions(x => executeRestRequest(composeQuery(x)))

  // Each request involves the composition of a query to the service. Inthis case, the query is delivered to the DW API
  def composeQuery(input: Iterator[String]): Iterator[String] = {
    var queryList = List[String]()
    while(input.hasNext) {

      // val dw_query = "http://www.dw.com/api/search/global?terms=" + "madrid" + "&languageId=" + input.next() + "&contentTypes=Article,Video&startDate=2012-01-01" + "&endDate=" + "2015-10-31" + "&sortByDate=true&pageIndex=1&asTeaser=false"

      val temp_q = "http://www.dw.com/api/search/global?terms=madrid&languageId=" + input.next() +
        "&contentTypes=Article,Video&startDate=2012-06-01&endDate=2015-06-22&sortByDate=true&pageIndex=1&asTeaser=false"

      println(temp_q)

      queryList .::= (temp_q)
    }

    queryList.iterator
  }


  // Each query is delivered to the service and the response is stored
  def executeRestRequest(query: Iterator[String]): Iterator[String] = {

    var queryResponse = List[String]()
    while (query.hasNext) {
      // The REST service is queried and the response (JSON format) is obtained
      val response: HttpResponse[String] = Http(query.next()).timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
        .asString
      // The response in JSON format is processed
      if (response.isNotError)
        queryResponse .::= (response.body)
    }
    queryResponse.iterator
  }

}
