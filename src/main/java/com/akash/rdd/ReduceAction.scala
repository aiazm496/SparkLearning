package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ReduceAction {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //create a range in scala and load it in RDD
    //sum it up using reduce

    val range = 1 to 10

    val sc = new SparkContext("local[*]", "Parallelize list")

    val rdd = sc.parallelize(range) //Rdd[Int]

    val reduceResult = rdd.reduce((x, y) => x + y) //action so result is local
    println(reduceResult) //55

    scala.io.StdIn.readLine()

  }
}
