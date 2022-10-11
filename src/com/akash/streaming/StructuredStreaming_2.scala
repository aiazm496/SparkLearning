package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreaming_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions",3)
      .config("spark.sql.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.streaming.schemaInference","true") //to infer schema of json
      .getOrCreate()

    //1. read file from folder as part of streaming (if 3 files in trigger then 3 are processed)
    val ordersDf = spark.readStream.format("json")
      .option("path","C:/TrendyTech/week16_downloads/ordersInputStream")
      //      .option("maxFilesPerTrigger",1)
      .load

    ordersDf.createOrReplaceTempView("orders")

    val completedOrdersDf = spark.sql("select * from orders where order_status = 'COMPLETE' ")

    //use append mode so that for each json input stream file, we get output only for that
    val ordersQuery = completedOrdersDf.writeStream.format("json")
      .outputMode("append")
      .option("path","C:/TrendyTech/week16_downloads/ordersOutputofInputStream")
      .option("checkpointLocation","checkpoint-location2")
      .trigger(Trigger.ProcessingTime("30 seconds")) //after 30 seconds micro batch is made and job is triggered.
      .start //start is action

    //in checkpoint-location2/sources we get file for each batchid and the file it processed is mentioned.
    //if we run the program again, it will skip processing input files as it already
    //has checkpointed those and will not process again.
    ordersQuery.awaitTermination()
    spark.stop()
  }
}
