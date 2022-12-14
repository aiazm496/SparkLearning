package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object StructuredStreaming_6 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("streamJoin").master("local[*]")
    .config("spark.sql.shuffle.partitions",3).config("spark.sql.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference","true") //to infer schema of json
    .getOrCreate()

  val impressionSchema = StructType(List(StructField("impressionID",StringType),
    StructField("ImpressionTime",TimestampType),
    StructField("CampaignName",StringType)))

  val clickSchema = StructType(List(StructField("clickID",StringType),
    StructField("ClickTime",TimestampType)))

  val impressionStreamDf = spark.readStream.format("json")
    .option("path","C:/TrendyTech/week16_downloads/streamJoinImpressionInput")
    .schema(impressionSchema).load

  //root
  // |-- impressionID: string (nullable = true)
  // |-- ImpressionTime: timestamp (nullable = true)
  // |-- CampaignName: string (nullable = true)

  val impressionStreamWatermarkedDf = impressionStreamDf.select("*")
    .withWatermark("ImpressionTime","30 minute")

  val clickStreamDf = spark.readStream.format("json")
    .option("path","C:/TrendyTech/week16_downloads/streamJoinClickInput")
    .schema(clickSchema).load
  //root
  // |-- clickID: string (nullable = true)
  // |-- ClickTime: timestamp (nullable = true)

  val clickStreamWatermarkedDf = clickStreamDf.select("*")
    .withWatermark("ClickTime","30 minute")

  //spark will calculate the watermark time based on the max event time (ImpressionTime) in each batch
  //processing.
  //watermark time will be 30minutes less of that time.
  //those records before watermark time will be cleared from state store.

  impressionStreamDf.printSchema()
  clickStreamDf.printSchema()

  val joinedDf = impressionStreamWatermarkedDf.join(clickStreamWatermarkedDf,
    expr("impressionID = clickID and ClickTime between  ImpressionTime and " +
      "ImpressionTime + interval 15 minute"),"inner")

  val joinQuery = joinedDf.writeStream.format("console")
    .outputMode("append")
    .option("checkpointLocation","checkpoint-location7").trigger(Trigger.ProcessingTime("15 seconds"))
    .start

  //watermark boundary max event time in impressions(imp1) is 12:00 -> 11:30 ( minus 30mins)
  //watermark boundary max event time in clicks(c1) is 14:18 -> 13:48 ( minus 30mins)
  //as per cleaning , it will remove all impression records before 11:30 and click records
  //before click time of 13:48.

  joinQuery.awaitTermination
  spark.stop


}
