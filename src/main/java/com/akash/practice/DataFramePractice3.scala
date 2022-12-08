package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFramePractice3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val windowDf = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("path", "C:/TrendyTech/week12_downloads/windowdata_with_header.csv").load

    windowDf.filter("country = 'Spain' ").show(5)

    windowDf.createOrReplaceTempView("invoice")

    spark.sql("select country, weeknum, invoicevalue, sum(invoicevalue) over(partition by country " +
      "order by weeknum) as runningSum from invoice").show()

  }
}
