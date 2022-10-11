package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFullReduceByWindow extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "stateful")

  val ssc = new StreamingContext(sc, Seconds(2))
  //every 2 seconds , a new RDD is created

  //to save previous data.
  ssc.checkpoint("C:/Users/akashpc/IdeaProjects/SparkWordCount/checkpoint")

  //Dstream containing rdds
  val lines = ssc.socketTextStream("localhost", 9999)

  val wordCounts = lines.reduceByWindow((x, y) => (x.toInt + y.toInt).toString,
    (x, y) => (x.toInt - y.toInt).toString, Seconds(10), Seconds(2))

  //similarly we can use countByWindow to find total count
  wordCounts.print()

  ssc.start() //action

  ssc.awaitTermination()

}
