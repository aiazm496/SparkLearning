package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object StructuredStreaming_4 {

  def main(args: Array[String]): Unit = {


    //we will process json transaction streaming data and join it with static df containing
    //member details of the card_id used in transactions.
    //This is called as stream enrichment, where we are adding some info to stream processed data
    //to get insights from stream.

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

    val joinDf = transactionDf.join(memberDf,
      transactionDf.col("card_id") === memberDf.col("card_id"), "left")
    //.drop(memberDf.col("card_id")
    //left table is streaming df and right table is static df

    //complete mode doesn't work with select queries like join is also, as it creates state store.
    //once a batch is processed, then as it is not maintained by state store, we do fresh
    //join in batch processing and display results.
    val joinedQuery = joinDf.writeStream.format("console").outputMode("update")
      .option("checkpointLocation", "checkpoint-location5")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    //when joining left, left df should be streamed df. ( in our case transactionDf should be left placed)
    //when joining right, right df should be streamed df.( it should be memberDf.join(transactionDf(right))
    joinedQuery.awaitTermination()

    spark.stop()
  }
}