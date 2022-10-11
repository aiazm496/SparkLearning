package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SaveMode, SparkSession}
object WaysToSelectColumns {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.master", "local[*]")
    sparkConfig.set("spark.app.name", "Rough")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val orderDf = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("path", "C:/TrendyTech/week12_downloads/order_data.csv").load

    //can use sql like syntax.
    orderDf.selectExpr("StockCode", "Quantity", "cast(UnitPrice as int) as UnitPriceInt ")
      .show(4)

    //using pure sql directly on df view.
    orderDf.createOrReplaceTempView("orders")
    val result = spark.sql("select StockCode,Quantity,cast(UnitPrice as int) as UnitPriceInt" +
      " from orders")
    result.show(4)
  }

}
