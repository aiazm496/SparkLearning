package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

//if age is > 18 the add Y else N ( new column)
object Assignment1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Assignment-1")

    val input = sc.textFile("C:/TrendyTech/week9_downloads/dataset1.dataset1")
    //rdd["sumit,30,bangalore"]

    val solve = input.map(x => {
      val age = x.split(",")(1).toInt
      val answer = if (age > 18) "Y" else "N"
      x + "," + answer
    })
    //rdd["sumit,30,bangalore,Y"]

    solve.collect().foreach(println)
  }
}
