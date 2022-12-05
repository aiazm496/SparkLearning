package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofweek, expr}
import org.apache.spark.sql.types.TimestampType

object AddNewColumnDf_3 {

  def year(datetime: String): String = {
    datetime.split(" ")(0).split("-")(0)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "rough")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val bigLogDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/biglog.txt")
      .option("header", true)
      .load

    bigLogDf.show(3)

    spark.udf.register("yearFinder", year(_: String))

    bigLogDf.withColumn("year", expr("yearFinder(datetime)"))
      .withColumn("dayOfweek", dayofweek(col("datetime").cast(TimestampType))).show(5)

  }
}
