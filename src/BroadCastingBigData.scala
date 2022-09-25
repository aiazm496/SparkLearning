import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source

object BroadCastingBigData {

  def loadBoringWords() = {

    val lines = Source.fromFile("C:/TrendyTech/week10_downloads/boredwords.txt").getLines()
    //iterator[String] iterator of lines in string

    var boringWords:Set[String] = Set() //declare as var to append
    lines.foreach(x=> boringWords+=x)

    boringWords //returns set of boring words
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Rough")

    val nameSet = sc.broadcast(loadBoringWords) //broadcast set of boring words

    val inputRdd = sc.textFile("C:/TrendyTech/week10_downloads/bigdatacampaigndata.csv")
    //Rdd collection of string
    //Rdd["asda.,ad",...]

    //      val flatmap = inputRdd.flatMap(x=>x.split(","))

    val mappedInput = inputRdd.map(x=> (x.split(",")(10).toFloat,x.split(",")(0)))
    //Rdd[(24.06,big data contents)]

    val flatMapValuesRdd = mappedInput.flatMapValues(x=>x.split(" "))  //x is string i.e. value
    //(24.06,big)
    //(24.06,data)
    //(24.06,contents)

    //flatMapValuesRdd.collect().foreach(println)

    val requiredRdd = flatMapValuesRdd.map(x=>(x._2.toLowerCase(),x._1)) //rdd[(big,24.06)]
      .filter(x=> !nameSet.value(x._1))       //filter out string key in broadcasted set
      .reduceByKey((x,y)=>x+y)                //value method in broadcast is like contains
      .sortBy(x=>x._2,ascending = false)

    requiredRdd.take(5).foreach(println)
    //take only 5 elements

    scala.io.StdIn.readLine()
  }
}

