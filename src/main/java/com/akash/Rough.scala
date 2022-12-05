package com.akash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Rough {

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]")
      .config("spark.sql.shuffle.partitions",3).config("spark.sql.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.streaming.schemaInference","true") //to infer schema of json
      .getOrCreate()

    val schema = StructType(List(StructField("order_id",IntegerType),StructField("order_date",TimestampType),
      StructField("order_customer_id",IntegerType),StructField("order_status",StringType),StructField("amount",IntegerType)))

    val ordersDf = spark.readStream.format("json").option("header",true)
      .option("path","C:/TrendyTech/week16_downloads/ordersInputStream").schema(schema).load
      .withWatermark("order_date","20 minute")

    val ordersDf2 = ordersDf.groupBy("order_status").count()

    val ordersQuery = ordersDf2.writeStream.format("console").outputMode("update")
      .option("checkpointLocation","checkpoint-location10")
      .option("path","C:/TrendyTech/week16_downloads/ordersOutputofInputStream")
      .option("header",true)
      .trigger(Trigger.ProcessingTime("20 Seconds")).start()

    ordersQuery.awaitTermination()

    spark.stop()

  }
}
