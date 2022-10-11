package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{broadcast, expr}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DfJoin3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val ordersDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/orders.csv")
      .option("header", true).load

    //rename column order_customer_id to customer_id to create ambiguity
    val orderRmDf = ordersDf.withColumnRenamed("order_customer_id",
      "customer_id")

    val customersDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/customers.csv")
      .option("header", true).load

    //we want to invoke broadcast join and broadcase customersDf as it is smaller df
    //compared to ordersDf

    val joindf = orderRmDf.join(broadcast(customersDf),
      orderRmDf.col("customer_id") === customersDf.col("customer_id"),
      "inner").drop(orderRmDf.col("customer_id"))
      .sort("customer_id")

    joindf.show()
    scala.io.StdIn.readLine()

  }
}
