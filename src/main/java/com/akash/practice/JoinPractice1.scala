package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object JoinPractice1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold","-1") //to avoid map side/broadcast join
      .getOrCreate()

    val ordersDf = spark.read.format("csv").option("path","C:/TrendyTech/week12_downloads/orders.csv")
      .option("header",true).option("inferSchema",true).load

    val customersDf = spark.read.format("csv").option("path","C:/TrendyTech/week12_downloads/customers.csv")
      .option("header",true).option("inferSchema",true).load

//    ordersDf.show(4)
//    customersDf.show(4)
    val joinedDf = customersDf.join(ordersDf,expr("order_customer_id = customer_id"),"right")

    println("no of partitions: " + joinedDf.rdd.getNumPartitions)

    joinedDf.show()

    scala.io.StdIn.readLine()
    spark.stop()

  }

}
