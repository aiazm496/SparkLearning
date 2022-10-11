package com.akash.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Assignment_week15_problem1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "problem1")

  val ssc = new StreamingContext(sc, Seconds(2))

  //Dstream reading from socket
  val lines = ssc.socketTextStream("localhost", 9999)

  ssc.checkpoint("./checkpoint") //checkpointing to persist state

  def updateFunction(newValues: Seq[Int], previousValues: Option[Int]): Option[Int] = {
    val x = newValues.sum + previousValues.getOrElse(0)
    Some(x)
  }

  val bigWords = lines.flatMap(_.split(" ")).map((_, 1)).filter(_._1.startsWith("big"))
  //contains only big key -> Dstream[(big,1), (biggest,1)..]

  val finalWords = bigWords.updateStateByKey(updateFunction) //contains words start with key and total

  val result = finalWords.map(_._1)

  result.print

  ssc.start()

  ssc.awaitTermination()

}
