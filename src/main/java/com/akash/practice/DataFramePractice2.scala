package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFramePractice2 {

  def checkIfOrderMoreThan500(amount:Long):String = if(amount >=500L) "good" else "bad"


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]")
      .getOrCreate()

    val ordersDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week16_downloads/ordersInputStream/jsonMessage.txt")
      .option("inferSchema", true).load

    ordersDf.printSchema()
    ordersDf.show()


    ordersDf.createOrReplaceTempView("orders")

    spark.sql("select * from orders").show()

    spark.sql("select *, case when amount >= 500 then 'good' else 'bad' end as indicator from orders ").show()

    spark.udf.register("checkAmount",checkIfOrderMoreThan500(_:Long))

    spark.sql("select *, checkAmount(amount) as indicator from orders").show()

    ordersDf.withColumn("dayofweek",dayofweek(col("order_date"))).show()

    ordersDf.withColumn("date", to_date(col("order_date"))).show()

    ordersDf.withColumn("diff_date_format",date_format(col("order_date"),"M-d-Y")).show()

    ordersDf.withColumn("id",monotonically_increasing_id())
      .withColumn("unixtimestamp",unix_timestamp(col("order_date"))).show()

    ordersDf.selectExpr("order_date","amount","year(order_date) as yr").show()

  }

}
