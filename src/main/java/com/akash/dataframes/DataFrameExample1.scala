package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExample1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "df1")
    sparkConf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //instead of rdd, we create spark dataframe
    val ordersDf = spark.read.option("header", true).
      option("inferSchema", true).csv("C:/TrendyTech/week11_downloads/orders.csv")
    //read is action //header true will make first row in csv as header.

    val groupedOrdersDf = ordersDf
      .repartition(4)
      .where("order_customer_id > 1000")
      .select("order_id", "order_customer_id")
      .groupBy("order_customer_id")
      .count()

    groupedOrdersDf.show()

    ordersDf.show() //by default it shows 20rows
    //show is action

    ordersDf.printSchema() //by default all columns type are string

    scala.io.StdIn.readLine()
    spark.stop()

  }
}
