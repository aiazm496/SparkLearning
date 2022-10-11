package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFullSparkWordCountStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "stateless")

  //creating spark streaming context, every 5 seconds a new RDD is created.
  val ssc = new StreamingContext(sc, Seconds(5))

  //lines is a DStream
  val lines = ssc.socketTextStream("localhost", 9990)
  //DStream of lines

  //to checkpoint state of created Rdds
  ssc.checkpoint(".") //use current dir

  val words = lines.flatMap(_.split(" "))

  val pairs = words.map((_, 1))
  //Dstread[("big",1),("hello",1)]

  //numValues => {1,1,1} => group key and contains its values
  //previousState is the previousBatch Rdd key totalCount
  def updateFunction(newValues: Seq[Int], previousState: Option[Int]): Option[Int] = {
    val newCount = newValues.sum + previousState.getOrElse(0)
    Some(newCount)
  }

  val wordsCount = pairs.updateStateByKey(updateFunction) //it considers all rdd data in dstream.

  wordsCount.print() //only display on batch (1 rdd sum)

  ssc.start()

  ssc.awaitTermination() //required else streaming app will stop

}
