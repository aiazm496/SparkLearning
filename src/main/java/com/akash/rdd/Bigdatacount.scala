package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

//filter out boring keywords
object Bigdatacount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Rough")

    val inputRdd = sc.textFile("C:/TrendyTech/week10_downloads/bigdatacampaigndata.csv")
    //Rdd collection of string
    //Rdd["asda.,ad",...]

    //      val flatmap = inputRdd.flatMap(x=>x.split(","))

    val mappedInput = inputRdd.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
    //Rdd[(24.06,big data contents)]

    val flatMapValuesRdd = mappedInput.flatMapValues(x => x.split(" ")) //x is string i.e. value
    //(24.06,big)
    //(24.06,data)
    //(24.06,contents)

    //flatMapValuesRdd.collect().foreach(println)

    val requiredRdd = flatMapValuesRdd.map(x => (x._2.toLowerCase(), x._1)).reduceByKey((x, y) => x + y)
      .sortBy(x => x._2, ascending = false)

    requiredRdd.take(5).foreach(println)
    //take only 5 elements
  }
}
