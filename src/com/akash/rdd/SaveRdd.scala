package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SaveRdd {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Save")

    val inputRdd = sc.textFile("C:/TrendyTech/week10_downloads/movies-201014-183159.dat")

    val resultRdd = inputRdd.map(x => (x.split("::")(2), 1))
      .reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false)

    //  resultRdd.saveAsTextFile("C:/TrendyTech/week10_downloads/movie-rate-count")

    val cnt = resultRdd.count() //action

    scala.io.StdIn.readLine();
  }
}
