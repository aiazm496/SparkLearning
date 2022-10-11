package com.akash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger

object Rough {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough")
      .master("local[*]")
      .getOrCreate()

    val df  = spark.read.format("csv").option("header",true)
    .option("path","C:/TrendyTech/bigDataset/full_dataset.tsv").load

    df.show(5,false)
    df.printSchema()

    //find the tweet_ids with maximum tweet
//    val groupedDf = df.groupBy("tweet_id").count().alias("count_tweets")
//      .sort(col("count_tweets").desc)
//
//    groupedDf.show(5,false)


    spark.stop()

  }
}
