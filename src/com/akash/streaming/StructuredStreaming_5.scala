package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object StructuredStreaming_5 extends App{
//  Logger.getLogger("org").setLevel(Level.ERROR)

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
  //as it is stateful transformation(stream join) important to clear state store through watermark.

  impressionStreamDf.printSchema()
  clickStreamDf.printSchema()

  val joinedDf = impressionStreamWatermarkedDf.join(clickStreamWatermarkedDf,
    impressionStreamWatermarkedDf.col("impressionID")===
      clickStreamWatermarkedDf.col("clickID"),"inner")

  val joinQuery = joinedDf.writeStream.format("console").outputMode("append")
    .option("checkpointLocation","checkpoint-location6").trigger(Trigger.ProcessingTime("15 seconds"))
    .start

  //watermark boundary max event time in impressions(imp1) is 12:00 -> 11:30 ( minus 30mins)
  //watermark boundary max event time in clicks(c1) is 14:18 -> 13:48 ( minus 30mins)
  //send 1 more impression(imp2) with imp time as 13:00 ( watermark -> 12:30) and click time(c2)
  // as 16:00 so, watermark -> 15:30. both imp1 and c1 should be cleared from state store.
  //try by sending c1 again, it should not match with any imp as imp1 was removed.

  joinQuery.awaitTermination
  spark.stop

  //if a record has not matched, then in future, if any match record which has not come comes,
  //then it joins as it stores data for both dfs in state store(disk and in executor memory).

}
