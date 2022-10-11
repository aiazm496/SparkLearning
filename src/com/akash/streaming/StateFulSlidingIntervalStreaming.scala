package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFulSlidingIntervalStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "stateful")

  val ssc = new StreamingContext(sc, Seconds(2))
  //every 2 seconds , a new RDD is created

  ssc.checkpoint("C:/Users/akashpc/IdeaProjects/SparkWordCount/checkpoint")

  //stateful
  val lines = ssc.socketTextStream("localhost", 9999)

  val wordCounts = lines
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(10), Seconds(2))
    .filter(_._2 > 0)
  //first arg is summary function-> add
  //seconds is inverse function -> subtract
  //third is window interval -> 10 secs ( we consider rdds data created in last 10sec(10/2) -> 5rdds
  //fourth is sliding interval-> every 2 seconds we remove an rdd.

  wordCounts.print

  ssc.start()

  ssc.awaitTermination()

}
