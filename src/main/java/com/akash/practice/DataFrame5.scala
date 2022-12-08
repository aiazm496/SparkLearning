package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrame5 {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]").getOrCreate()

    val ordersDf = spark.read.format("csv").option("path", "C:/TrendyTech/week12_downloads/orders.csv")
      .option("header",true).load

    ordersDf.show(5,false)

    ordersDf.write.format("json").option("path","C:/TrendyTech/week12_downloads/ordersDf").mode(SaveMode.Overwrite)
      .partitionBy("order_status")
      .save

  }
}
