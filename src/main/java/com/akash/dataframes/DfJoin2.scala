package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SaveMode, SparkSession}
object DfJoin2 {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().
      config(sparkConfig).getOrCreate()

    val ordersDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/orders.csv")
      .option("header", true).load

    //    ordersDf.show(2)

    //rename column order_customer_id to customer_id to create ambiguity
    val orderRmDf = ordersDf.withColumnRenamed("order_customer_id", "customer_id")


    val customersDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/customers.csv")
      .option("header", true).load

    //    customersDf.show(2)

    //    println(orderRmDf.rdd.getNumPartitions)
    //    println(customersDf.rdd.getNumPartitions)

    //total 2 tasks will run as we have 2 dataframes.
    //for a dataframe, in the executor, map happens and key,value is created with key as join col and value as all other cols.
    //this map is written to exchange in same machine which is a buffer.
    //mapped output of orders-> (2(customer_id),{2013-07-25 00:00:00.0,256,PENDING_PAYMENT}) in executor 1(stage 1).
    //mapped output of customers-> (2(customer_id),{Mary,Barrett,XXXXXXXXX,XXXXXXXXX,9526 Noble Embers Ridge,Littleton,CO,80126})
    //in executor 2(stage 2).
    //then shuffling happens, and then exchange data is shuffled to new executor 3(stage3).
    //where it is sorted(on keys), then it is merged and joined.(hence shuffle sort-merge-join)

    //however, in case of inner join spark may try to use mapsidejoin(broadcast join) -> no shuffling
    //the smaller table exchange data will be broadcasted to large table executor and broadcast join happens.
    //use broadcase join(map side join) when 1 table is small, so that it can be loaded in memory of big table executor.
    //if shuffling is avoided it is a huge performance gain as shuffling is heavy computation process.
    //to turn off broadcast join run spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

    //if right join, right table should be big


    //simple join
    //inner
    //after join drop ambiguous col customer_id
    //if order id is null(customer didn't order) show it as -1
    val joindf = orderRmDf.join(customersDf,
      orderRmDf.col("customer_id") === customersDf.col("customer_id"),
      "outer").drop(orderRmDf.col("customer_id"))
      .withColumn("order_id", expr("coalesce(order_id,-1)"))
      .sort("customer_id")

    //    println(joindf.rdd.getNumPartitions)

    joindf.show()

    scala.io.StdIn.readLine()
    spark.stop()
  }
}
