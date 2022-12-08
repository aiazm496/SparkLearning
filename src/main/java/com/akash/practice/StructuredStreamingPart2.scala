package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamingPart2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true") //to infer schema of json
      .getOrCreate()

    val ordersInputStreamDf = spark.readStream.format("json").option("path","C:/TrendyTech/week16_downloads/ordersInputStream").load

    val completedOrdersDf = ordersInputStreamDf.filter("order_status = 'COMPLETE'").writeStream.format("json").
    option("path","C:/TrendyTech/week16_downloads/ordersOutputofInputStream").option("checkpointLocation","practiceCheckpointPart2").
      trigger(Trigger.ProcessingTime("1 minute")).start()

    completedOrdersDf.awaitTermination()

  }
}
