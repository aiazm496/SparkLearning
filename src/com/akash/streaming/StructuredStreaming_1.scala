package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreaming_1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]").config("spark.streaming.stopGracefullyOnShutdown", "true") //for recovering data.
      .config("spark.sql.shuffle.partitions", 3) //after shuffle(group by) only 3 partitions are created
      //not 200, which saves time in processing empty data partitions.
      .getOrCreate()

    //read from stream
    val linesDf = spark.readStream.format("socket").option("host", "localhost")
      .option("port", "9999").load

    linesDf.printSchema()

    //processing
    val newDf = linesDf.selectExpr("explode(split(value,' ')) as word")
    val aggDf = newDf.groupBy("word").count()

    //write to sink
    //if we use update mode, it will only new+old if matched, in append mode only new values are
    //displayed in batch.
    //if trigger is not mentioned then after every input a new micro batch is created
    //and spark job is triggered and we get instant result(in 0.2 - 1 seconds if small data.)
    //with trigger, a micro batch is created after 30 seconds and then sent to spark for processing.
    //output mode complete, will include all previous record and sum of all old keys including new.
    val wordCountQuery = aggDf.writeStream.format("console")
      .outputMode("complete")
      .option("checkpointLocation", "checkpointlocation")
      .trigger(Trigger.ProcessingTime("30 Seconds"))
      .start
    //start is action
    //checkpoint is for storing old data. if we start spark again, it recovers data.
    wordCountQuery.awaitTermination()
  }

}
