package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCalculator {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MovieRatingsCalculator")

    val input = sc.textFile("C:/TrendyTech/week9_downloads/moviedata-201008-180523.data")
    //"196	242	3	881250949"

    val mappedInput = input.map(x => x.split("\t")(2))
    //RDD[String] = Rdd("1","2","3")

    //    val ratings = mappedInput.map(x=>(x,1))  // ("1",1), ("2",1), ("1",1)
    //
    //    val reducedRatings = ratings.reduceByKey((x,y)=>x+y)
    //
    //    val results = reducedRatings.collect() //action, stored in array

    //countByValue is action it is not lazy and doesn't create rdd
    //like reduceByKey which is a transformation
    //countByValue variable is local(not on spark cluster)
    //if we perform more operations on result we won't get parallelism
    //so countByValue should be the last thing and no more operations should
    //be performed
    val result = mappedInput.countByValue
    result.foreach(println)
  }
}
