package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CatalystOptimizer extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name","Catalyst-Optimizer")
  sparkConfig.set("spark.master","local[*]")

  val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

  val ordersDf = spark.read.format("csv").option("path","C:/TrendyTech/week11_downloads/orders.csv")
    .option("header",true).load

  ordersDf.show(2)

  ordersDf.createOrReplaceTempView("orders")

  spark.sql("select order_id from orders where order_id in (select order_id from" +
    " orders) and order_id < 5 ").explain(true)

  //catalyst optimizer works in df,ds and spark.sql which optimize sql execution plan
  //above only filter happens first then select order_id and skips inner query.

  scala.io.StdIn.readLine()
  spark.stop()

}
