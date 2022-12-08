package com.akash.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JsonWeatherParse {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("jsonParse").master("local[*]")
      .getOrCreate()

    val weatherRdd = spark.sparkContext.
      textFile("weather_store_hdfs/temp")

    //,"temperature":20,"weather..
    val temperatureRdd = weatherRdd.map(x => {
      val tempString = x.split(""""temperature":""")
      val temperature = tempString(1).split(",")(0).toInt
      temperature
    }
    )
    //Rdd[20]

    temperatureRdd.coalesce(1).saveAsTextFile("weather_store_hdfs_temperature")
  }

}
