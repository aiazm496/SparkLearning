package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StructuredStreamingPart1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]").config("spark.streaming.stopGracefullyOnShutdown", "true") //for recovering data.
      .config("spark.sql.shuffle.partitions", 3) //after shuffle(group by) only 3 partitions are created
      .getOrCreate()

    //C:\Program Files (x86)\Nmap>ncat -lk 9999      --> write
    val streamDf = spark.readStream.format("socket").option("host","localhost").option("port","9999").load()

    val streamQuery = streamDf.writeStream.format("console").start()

    streamQuery.awaitTermination()


  }
}
