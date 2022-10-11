package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR) //show only error logs,else it shows
    //huge spark logs

    val sc = new SparkContext("local[*]", "wordcount")
    //spark context to run scala code on spark cluster and get parallelism
    //local means run on local and * means use all cores


    val input = sc.textFile("C:/TrendyTech/week9_downloads/search.txt")
    //input is rdd[string] so collection of string of each line
    //text file read each line as a seperate string

    val words = input.flatMap(x => x.split(" "))
    //if we use map then it will return RDD of Array of Strings
    //if we use flatMap then it will flatten and return only RDD[Strings]

    val wordsLower = words.map(x => x.toLowerCase())
    //val wordsLower = words.map(_.toLowerCase())

    val wordMap = wordsLower.map(x => (x, 1)) //to pair big and Big together
    //RDD[tuple(String,Int)]

    val finalCount = wordMap.reduceByKey((x, y) => x + y)
    //val finalCount = wordMap.reduceByKey(_+_)

    //as there is only sortByKey method, we will interchange key and value
    val reversedTuple = finalCount.map(x => (x._2, x._1))

    val sortedResult = reversedTuple.sortByKey(ascending = false)
    //try sortBy

    val sortedResultDefaultKeyValue = sortedResult.map(x => (x._2, x._1))

    //action called by collect
    //collect will convert RDD of tuple of key and value to array. used to collect data from rdd.
    sortedResultDefaultKeyValue.collect().foreach(println(_))
    //(solution,2)

    //spark ui = localhost:4040

    //scala.io.StdIn.readLine() //to keep still run and see the spark ui /dags else once program has run
    //spark ui is down
  }
}
