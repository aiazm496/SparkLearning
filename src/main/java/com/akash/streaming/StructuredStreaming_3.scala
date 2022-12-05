package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreaming_3 {

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
      .load

    ordersDf.createOrReplaceTempView("orders")

//    val completedOrdersDf = spark.sql("select  count(*) from orders where order_status = 'COMPLETE' ")
      val completedOrdersDf = spark.sql("select order_status,count(*) as total_orders from orders " +
        "group by order_status")

    //use complete mode so that for each json input stream file, we get sum count of all previous
    // batches processed in new batch output.
    //if used update mode, then only old matched and new agg result will be displayed.
    //This is unbounded aggregation as there is no window and state is infinitely stored.
    val ordersQuery = completedOrdersDf.writeStream.format("console")
      .outputMode("update")
      .option("checkpointLocation","checkpoint-location4") //stores the batch file processed in
      // sources folder. in state folder we maintain the state/history like running total.(aggregations)
      //without state we can't do running total as it require history of prev batch.
      //it only happens in complete output mode.
      //however, the state information is read from memory of executor(state store) for quick batch processing.
      //so, we should be careful, as if history/state keeps growing we can get out of memory exception.
      //in checkpointing shuffling happens and result it written to disk and then read again and loaded
      //in executor memory.
      //we can cleanup state store manually in case aggregation is unbounded and never ending
      //like orders keep coming.
      //in case of time bound aggregations (window aggregates) -> spark do state cleanup post window
      //time is completed and batch for that window is processed.(job completed)
      //for example when orders expire in 1 month.
      //2 types of window aggregates -> 1)Tumbling time window (a series of fixed size, non-overlapping) :
      //a) 10-10:15 , 10:15-10:30 , 10:30-10:45
      //2)Sliding time window -> fixed size , but overlapping:
      // 1) 10-10:15, 10:05-10:20, 10:10- 10:25 ( fixed window interval 15min, 5min sliding window)
      //in commits we only get batch for which processing job has completed.
      .trigger(Trigger.ProcessingTime("15 seconds")) //after 15 seconds micro batch is made
      // and job is triggered.
      //if not trigger given then batch created instantly when input given.
      .start //start is action

    ordersQuery.awaitTermination()
    spark.stop()
  }
}
