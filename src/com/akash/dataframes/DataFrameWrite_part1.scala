package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrameWrite_part1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val ordersDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/orders.csv")
      .option("header", true).load()

    //overwrite mode delete existing folder
    ordersDf.write.format("json").mode(SaveMode.Overwrite).
      option("path", "C:/TrendyTech/week12_downloads/ordersDf")
      .save()

    //we get only 1 part file as num of partitions of dataframe is 1.
    //to check no of partitions in df, convert to rdd
    println(ordersDf.rdd.getNumPartitions)

    //change no of partitions hence no of output files using repartition to inc parallelism
    //repartition do full shuffle and divide data equally
    val orderDf2partitions = ordersDf.repartition(2)
    orderDf2partitions.write.format("json").mode(SaveMode.Overwrite).
      option("path", "C:/TrendyTech/week12_downloads/ordersDf2partitions")
      .save()

    println(orderDf2partitions.rdd.getNumPartitions)

    //use partition by to create partition folders like hive
    //2 files will be create in each partition by folder as we have 2 partitions
    orderDf2partitions.write.format("csv").mode(SaveMode.Overwrite)
      .option("path", "C:/TrendyTech/week12_downloads/ordersDfpartitionByOrderStatus")
      .partitionBy("order_status").save()


    //ordersdf has 66000 records .. max record per output file should be 5000.

    ordersDf.write.format("json").mode(SaveMode.Overwrite).
      option("path", "C:/TrendyTech/week12_downloads/ordersDf_max5000records")
      .option("maxRecordsPerFile", 5000)
      .save()


  }
}
