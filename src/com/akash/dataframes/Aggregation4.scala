package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Aggregation4 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "AggregationPivot")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val bigLogDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/biglog.txt")
      .option("header", true).load

    //    bigLogDf.show(2,false)
    //aggregate by level, monthname, count  SORTED BY monthname
    // DEBUG, JANUARY, 5
    // INFO , JANUARY , 20
    // DEBUG , FEBRUARY , 10

    bigLogDf.printSchema()
    bigLogDf.createOrReplaceTempView("inputview")

    val result = spark.sql("select level, date_format(datetime,'MMMM') as month_name " +
      "from inputview")

    result.show(10)

    //    val resultPivot = spark.sql("select level, date_format(datetime,'MMMM') as month_name " +
    //      "from inputview").groupBy("level").pivot("month_name").count()

    //spark optimization instead of using computation to calculate distinct months in pivot
    //supply a list of months

    val months = List("January", "February", "March", "April", "May", "June", "July", "August", "September",
      "October", "November", "December")

    val resultPivot = spark.sql("select level, date_format(datetime,'MMMM') as month_name " +
      "from inputview").groupBy("level").pivot("month_name", months).count()

    resultPivot.show()

  }
}
