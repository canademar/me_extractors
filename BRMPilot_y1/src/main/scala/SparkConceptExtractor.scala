import java.text.Normalizer

import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

/**
 * Created by cnavarro on 26/10/15.
 */
class SparkConceptExtractor (taxonomy: RDD[(String, Int)], inlinks_threshold: Int, window_max_length: Int) extends
Serializable{



  def extractConceptsFromRDD(input: RDD[String]): RDD[String] = {

    println("!111111111111111111111 Extract concpets from rdd")

    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]())).map(x => collection.mutable.Map(x.toSeq: _*))

    val temp2 = extractConcepts(temp)

    temp2.mapPartitions(x => {

      implicit val formats = Serialization.formats(NoTypeHints)

      var a = List[String]()
      while(x.hasNext) {
        val r = x.next()
        a=a:+(write(r))
      }
      a.toIterator

    })

  }

  def extractConcepts(inputRDD: RDD[scala.collection.mutable.Map[String,Any]]): RDD[scala.collection.mutable
  .Map[String,Any]] ={



    val inputIndexMap = inputRDD.zipWithIndex() // origMap, index

    val filtTax = taxonomy.filter(x=>x._2>inlinks_threshold).map(x=>(x._1,Set.empty))

    val filteredInput = inputIndexMap.filter(x=>x._1.getOrElse("lang","")=="es")

    println("Concepts for something")

    val input = filteredInput.map(x=>(x._1.getOrElse("text","").asInstanceOf[String],defineSubentities(x._1.getOrElse("text", "").asInstanceOf[String]), x._2)) // origMap, submessages, index
    //val input = inputIndexMap.map(x=>(x._1.getOrElse("text","").asInstanceOf[String],defineSubentities(x._1.getOrElse("text", "").asInstanceOf[String]), x._2)) // origMap, submessages, index

    println("some map")

    val tempInput = input.map(x=>(x._2, x._3)).map(x=>{
      var tempSet = scala.collection.mutable.Set[(String, Long)]()
      for(elem<-x._1) {
        tempSet += ((elem, x._2))
      }
      tempSet.toArray
    }).flatMap(x=>x).groupByKey().map(x=>(x._1,x._2.toSet)) // submessages, set of indices

    val tempResult = tempInput.join(filtTax).map(x=> (x._1,x._2._1 ++ x._2._2)).map(x=>{
      var tempSet = scala.collection.mutable.Set[(String, Long)]()
      for(index<-x._2) {
        tempSet += ((x._1, index))
      }
      tempSet.toArray

    }).flatMap(x=>x).map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.toList)) //Index, list of concepts

    println("Now only remains to format")

    val result = inputIndexMap.map(x => (x._2, x._1)).cogroup(tempResult).map(x=> (x._2._1.iterator.next()
      .asInstanceOf[scala.collection.mutable.Map[String, Any]], x._2._2.toList.flatMap(y=>y).distinct)).map(x=> {

      /*
      val old_source = collection.mutable.Map(x._1.get("_source").asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]()).toSeq: _*)
      val new_source = (old_source += (("concepts")->x._2.toArray)).asInstanceOf[scala.collection.mutable.Map[String, Any]]
      */
      val new_x = x._1 + ("concepts" -> x._2.toArray)
      new_x
    })

    result

  }

  def defineSubentities(text: String):List[String] = {
    var start = 0
    var length = 1
    var subsegments = scala.collection.mutable.Set[String]()
    val clean_text = BasicConceptExtractor.cleanText(text)
    //println("\n\nClean text: " + clean_text)
    val words = clean_text.split("""[\s\.\n"',()]""")
    //words.foreach(w => println("Word: " + w))

    while(start+length<=words.length){
      length = 1
      while(length<=window_max_length && length<=(words.length-start)) {
        val phrase = words.slice(start, start + length).mkString(" ")
        subsegments += phrase
        length += 1
      }
      start += 1
      length = 1
    }
    for(concept<-subsegments){
      for(otherConcept<-subsegments){
        if(concept!=otherConcept & (concept contains otherConcept)){
          subsegments.-(otherConcept)
        }
      }
    }

    subsegments.toList

  }



  def removeAccents(text: String): String ={
    val str = Normalizer.normalize(text,Normalizer.Form.NFD)
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    exp.replaceAllIn(str,"")
  }



}

object SparkConceptExtractor{
  def cleanText(text: String): String ={
    val cleanText = removeAccents(text.toLowerCase)
    cleanText.replaceAll("""\s+""", " ")
  }

  def removeAccents(text: String): String ={
    val str = Normalizer.normalize(text,Normalizer.Form.NFD)
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    exp.replaceAllIn(str,"")
  }

  /*
    def extractConcepts(inputRDD: RDD[Map[String,Any]]): RDD[(String,List[String])] ={

      val filtTax = taxonomy.filter(x=>x._2>inlinks_threshold).map(x=>(x._1, x._1)).collect().toMap

      val input = inputRDD.map(x=>(x.getOrElse("text", "").asInstanceOf[String],defineSubentities(x.getOrElse("text","").asInstanceOf[String])))

      val result = input.map(x=> (x._1, {
        val subElems = x._2
        var resSet = scala.collection.mutable.Set[String]()
        for(elem<-subElems) {
          if(filtTax.getOrElse(elem,"").asInstanceOf[String].compareTo("") != 0)
            resSet += filtTax.getOrElse(elem,"")
        }

        resSet.toList
      }))

      result

    }

    def extractConcepts2(inputRDD: RDD[Map[String,Any]]): RDD[(String,List[String])] ={

      val filtTax = taxonomy.filter(x=>x._2>inlinks_threshold).map(x=>(x._1,Set.empty))

      val input = inputRDD.map(x=>(x.getOrElse("text", "").asInstanceOf[String],defineSubentities(x.getOrElse("text",
        "").asInstanceOf[String]))).zipWithIndex() // message, submessages, index

      val input2 = input.map(x=>(x._2,x._1._1)) // index, string

      val tempInput = input.map(x=>(x._1._2, x._2)).map(x=>{
        var tempSet = scala.collection.mutable.Set[(String, Long)]()
        for(elem<-x._1) {
          tempSet += ((elem, x._2))
        }
        tempSet.toArray
      }).flatMap(x=>x).groupByKey().map(x=>(x._1,x._2.toSet)) // submessages, set of indices

      val tempResult = tempInput.join(filtTax).map(x=> (x._1,x._2._1 ++ x._2._2)).map(x=>{
        var tempSet = scala.collection.mutable.Set[(String, Long)]()
        for(index<-x._2) {
          tempSet += ((x._1, index))
        }
        tempSet.toArray

      }).flatMap(x=>x).map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.toList)) //Index, list of concepts

      val result = input2.cogroup(tempResult).map(x=>(x._2._1.toList.toString(), x._2._2.toList.flatMap(y=>y).distinct))

      // TODO: añadir al map de entrada un campo concepts con la lista de conceptos identificados

      result

    }

    def extractConcepts3(inputRDD: RDD[scala.collection.mutable.Map[String,Any]]): RDD[scala.collection.mutable
    .Map[String,Any]] ={

      val inputIndexMap = inputRDD.zipWithIndex() // map, index

      val filtTax = taxonomy.filter(x=>x._2>inlinks_threshold).map(x=>(x._1,Set.empty))

      val input = inputIndexMap.map(x=>(x._1.getOrElse("text", "").asInstanceOf[String],defineSubentities(x._1.getOrElse
        ("text","").asInstanceOf[String]), x._2)) // message, submessages, index

      val input2 = input.map(x=>(x._3,x._1)) // index, string

      val tempInput = input.map(x=>(x._2, x._3)).map(x=>{
        var tempSet = scala.collection.mutable.Set[(String, Long)]()
        for(elem<-x._1) {
          tempSet += ((elem, x._2))
        }
        tempSet.toArray
      }).flatMap(x=>x).groupByKey().map(x=>(x._1,x._2.toSet)) // submessages, set of indices

      val tempResult = tempInput.join(filtTax).map(x=> (x._1,x._2._1 ++ x._2._2)).map(x=>{
        var tempSet = scala.collection.mutable.Set[(String, Long)]()
        for(index<-x._2) {
          tempSet += ((x._1, index))
        }
        tempSet.toArray

      }).flatMap(x=>x).map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.toList)) //Index, list of concepts

      val result = inputIndexMap.map(x => (x._2, x._1)).cogroup(tempResult).map(x=> (x._2._1.iterator.next()
        .asInstanceOf[scala.collection.mutable.Map[String, Any]], x._2._2.toList.flatMap(y=>y).distinct)).map(x=>(x._1
        += ("concepts")->x._2.toArray).asInstanceOf[scala.collection.mutable.Map[String, Any]])


      result

    }
    */

  /*
  def extractConceptsFromRDDofJSON(input: RDD[String]): RDD[String] = {

    input.foreach("Inside extract concepts from json: " + println(_))

    val temp = input.map(x=> JSON.parseFull(x).asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]()).get
      ("_source").asInstanceOf[Some[Map[String,Any]]].getOrElse(Map[String, Any]())).map(x => collection.mutable.Map
      (x.toSeq: _*))

    val temp2 = extractConcepts3(temp)

    temp2.mapPartitions(x => {

      implicit val formats = Serialization.formats(NoTypeHints)

      var a = List[String]()
      while(x.hasNext) {
        val r = x.next()
        a=a:+(write(r))
      }
      a.toIterator

    })

  }
  */

/*
  def extractConceptForEntry(text: String): List[String] ={
    var start = 0
    var length = 1
    var concepts = scala.collection.mutable.Set[String]()
    val clean_text = BasicConceptExtractor.cleanText(text)
    //println("\n\nClean text: " + clean_text)
    val words = clean_text.split("""[\s\.\n"',()]""")
    //words.foreach(w => println("Word: " + w))

    while(start+length<=words.length){
      length = 1
      while(length<=window_max_length && length<=(words.length-start)) {
        val phrase = words.slice(start, start + length).mkString(" ")
        val m = taxonomy.lookup(phrase).toArray
        if(m.length==1)
           if(m(0) > inlinks_threshold)
             concepts += phrase

        length += 1
      }
      start += 1
      length = 1
    }
    for(concept<-concepts){
      for(otherConcept<-concepts){
        if(concept!=otherConcept & (concept contains otherConcept)){
          concepts.-(otherConcept)
        }
      }
    }

    concepts.toList

  }
  */



}
