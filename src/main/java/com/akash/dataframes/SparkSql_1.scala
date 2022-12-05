package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql_1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "SparkSql")
  sparkConfig.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

  val ordersDf = spark.read.format("csv")
    .option("path", "C:/TrendyTech/week12_downloads/orders.csv")
    .option("header", true).load()

  //count of order_status using spark_sql
  //creates temporary view of dataframe in-memory. It is removed after end of
  //spark session.
  ordersDf.createOrReplaceTempView("orders") //spark sql will refer this

  //spark sql returns dataframe
  //it also used catalyst optimizer like dataframe to manage performance of queries.
  //(join,indexing optimization etc)
  val countOrderStatusDf = spark.sql("select order_status,count(*) as total_cnt from " +
    "orders group by order_status order by total_cnt desc ")

  countOrderStatusDf.show()

  //count order_status using inbuilt methods
  //ordersDf.groupBy("order_status").count().show()

}
