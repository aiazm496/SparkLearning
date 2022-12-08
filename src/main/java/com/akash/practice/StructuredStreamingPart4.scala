package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object StructuredStreamingPart4 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("streamEnrichment").master("local[*]")
      .config("spark.sql.shuffle.partitions", 3).config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true") //to infer schema of json
      .getOrCreate()

    //{"card_id":5572427538311236,"amount":5358617,"postcode":41015,"pos_id":970896588019984,"transaction_dt":"2018
    //json schema
    val schemaTransaction = StructType(List(StructField("card_id", LongType), StructField("amount", IntegerType),
      StructField("postcode", IntegerType), StructField("pos_id", LongType), StructField("transaction_dt", StringType)))

    //streaming data df
    val transactionDf = spark.readStream.format("json")
      .option("path", "C:/TrendyTech/week16_downloads/transactionInput")
      .schema(schemaTransaction)
      .load

    //static df
    val memberDf = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week16_downloads/members.csv").option("header", true)
      .option("inferSchema", true)
      .load

    val joinedDf = transactionDf.join(memberDf,transactionDf.col("card_id")===memberDf.col("card_id"),"left")

    //complete mode wont work as state store not maintained for stream and static join.
    val query = joinedDf.writeStream.format("console").option("checkpointLocation","practice4checkpoint")
      .trigger(Trigger.ProcessingTime("30 seconds")).start()

    query.awaitTermination()

  }
}
