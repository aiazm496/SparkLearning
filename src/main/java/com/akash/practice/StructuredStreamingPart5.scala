package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamingPart5 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("streamEnrichment").master("local[*]")
      .config("spark.sql.shuffle.partitions", 3).config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true") //to infer schema of json
      .getOrCreate()

    val impressionDf = spark.readStream.format("json").option("path","C:/TrendyTech/week16_downloads/streamJoinImpressionInput").load

    val clickDf = spark.readStream.format("json").option("path","C:/TrendyTech/week16_downloads/streamJoinClickInput").load

    val joinedDf = impressionDf.join(clickDf, expr("impressionId = clickID"))

    val query = joinedDf.writeStream.format("console").option("checkpointLocation","practice5checkpoint")
      .trigger(Trigger.ProcessingTime("30 seconds")).start()
    //state store is created when joiend df are both stream, so df are stored in memory of executor.

    query.awaitTermination()

  }

}
