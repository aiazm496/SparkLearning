package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Assignment_week15_problem3 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","rough")

  val ssc = new StreamingContext(sc,Seconds(2))

  val dstream = ssc.socketTextStream("localhost",9999)

  ssc.checkpoint("/delete2")

  val result = dstream.countByWindow(Seconds(10),Seconds(2))

  result.print()

  ssc.start()

  ssc.awaitTermination()
}
