package com.akash.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BigLogReduceByKey extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("BigLog").master("local[*]").getOrCreate()

    val rdd = spark.sparkContext.textFile("C:/TrendyTech/week18_downloads/bigLogNew.txt").coalesce(16)
    //44 partitions created by default so 44 tasks will run, max 16 executor
    //RDD["ERROR: Thu Jun 04 10:37:51 BST 2015"]

    println(rdd.getNumPartitions)

    val keyValueRdd = rdd.map(log => (log.split(":")(0),1))
    //RDD[("ERROR",1)]

    val result = keyValueRdd.reduceByKey(_+_)
    //shuffling happens and same 44 partitions will be created, but only 2 partitions will have data as we have only 2 unique keys
    // ERROR and WARN.

    println(result.getNumPartitions)

    result.coalesce(1).saveAsTextFile("C:/TrendyTech/week18_downloads/bigLogReduceByKey")
    //instead of having 16 partition files we make 1 partition.

    scala.io.StdIn.readLine()
    spark.stop()

}
