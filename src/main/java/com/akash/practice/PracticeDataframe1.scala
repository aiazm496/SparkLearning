package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object PracticeDataframe1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]")
      //      .config("spark.sql.shuffle.partitions",3)
      //      .config("spark.sql.streaming.stopGracefullyOnShutdown","true")
      //      .config("spark.sql.streaming.schemaInference","true") //to infer schema of json
      .getOrCreate()

    val ordersDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week16_downloads/ordersInputStream/jsonMessage.txt")
      .option("inferSchema", true).load

    ordersDf.printSchema()

    ordersDf.sort(col("amount").desc, col("order_date"))
      .withColumn("dayofweek", expr("dayofweek(order_date)")).show()

    //find the total order value of customer
    ordersDf.createOrReplaceTempView("orders")

    val ordersDfSparkSql = spark.sql("select *, dayofweek(order_date) as dayofweek from orders " +
      "order by amount desc, order_date")

    ordersDfSparkSql.show()

    //give rank to customer wrt amount

    spark.sql("select *, rank() over(order by amount desc) as rnk from orders ").show()

    ordersDf.groupBy("order_customer_id").sum("amount").as("total_amount").show()

    val groupOrdersDf = spark.sql("select order_customer_id, sum(amount) as sm from orders group by " +
      "order_customer_id order by sm desc")

    groupOrdersDf.show()

    //define schema
    val customSchema = "order_id int, order_date string, order_customer_id int, order_status string, amount int"

    val ordersDfCustom = spark.read.format("json")
      .option("path", "C:/TrendyTech/week16_downloads/ordersInputStream/jsonMessage.txt")
      .schema(customSchema).load

    ordersDfCustom.printSchema()

    ordersDfCustom.show()

    val manualSchema = StructType(List(StructField("order_id", IntegerType)
      , StructField("order_date", StringType),
      StructField("order_customer_id", IntegerType), StructField("order_status", StringType),
      StructField("amount", IntegerType)
    ))

    val ordersDfManual = spark.read.format("json")
      .option("path", "C:/TrendyTech/week16_downloads/ordersInputStream/jsonMessage.txt")
      .schema(manualSchema).load

    ordersDfManual.printSchema()

    ordersDfManual.show()

  }
}
