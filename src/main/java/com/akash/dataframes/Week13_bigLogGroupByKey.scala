package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object Week13_bigLogGroupByKey {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "GroupByKey")

    val rdd = sc.textFile("C:/TrendyTech/week13_downloads/bigLogNew.txt")

    val rdd2 = rdd.map(x => (x.split(":")(0), x.split(":")(1)))
    //rdd will have partitions. if hdfs files then partitions = no of blocks

    //RDD[( ERROR,Thu Jun 04 10:37:51 BST 2015)]

    val rdd3 = rdd2.groupByKey //all key,values are shuffled to a new
    // executor in beginning itself
    //RDD[(ERROR, Iterable all values of ERROR)]

    val rdd4 = rdd3.map(x => (x._1, x._2.size))

    rdd4.collect()

  }
}
