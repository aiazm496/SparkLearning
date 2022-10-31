package com.akash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger

object Rough {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]").config("spark.sql.shuffle.partitions",3)
      .getOrCreate()

    val df  = spark.read.format("csv").option("header",true)
    .option("path","C:/TrendyTech/week16_downloads/members.csv").load

    println(df.rdd.getNumPartitions)

    println(df.count())

    scala.io.StdIn.readLine()
    spark.stop()

  }
}
