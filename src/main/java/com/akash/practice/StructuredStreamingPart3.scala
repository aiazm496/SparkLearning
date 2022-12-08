package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object StructuredStreamingPart3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val schema = StructType(List(StructField("order_id", IntegerType),
      StructField("order_date", TimestampType),
      StructField("order_customer_id", IntegerType),
      StructField("order_status", StringType),
      StructField("amount", IntegerType)))

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true") //to infer schema of json
      .getOrCreate()

    val ordersDf = spark.readStream.format("json").option("path", "C:/TrendyTech/week16_downloads/ordersInputStream").
      schema(schema).load

    val watermarkedDf = ordersDf.withWatermark("order_date", "30 minute")

    watermarkedDf.createOrReplaceTempView("orders")

    val ordersGroupedDf = spark.sql("select order_status,count(*) as total_orders from orders group by order_status")

    val query = ordersGroupedDf.writeStream.format("console").outputMode("COMPLETE").option("checkpointLocation", "practiceCheckpointPart3")
      .trigger(Trigger.ProcessingTime("15 seconds")).start()

    query.awaitTermination()

  }
}
