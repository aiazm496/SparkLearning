package com.akash.aws
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

//write spark program to process json(2gb), create dataframe.


object AmazonReviews {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //16 core machine so at max 16 tasks can run in parallel.
    val spark = SparkSession.builder().appName("BigLog").master("local[*]").getOrCreate()

    var reviewsRawDf = spark.read.format("json").option("path","C:/TrendyTech/bigDataset/Cell_Phones_and_Accessories.json").load

    println(reviewsRawDf.rdd.getNumPartitions)

    reviewsRawDf.printSchema()
//    root
//    |-- _id: struct (nullable = true)
//    |    |-- $oid: string (nullable = true)
//    |-- asin: string (nullable = true)
//    |-- category: string (nullable = true)
//    |-- class: double (nullable = true)
//    |-- helpful: array (nullable = true)
//    |    |-- element: long (containsNull = true)
//    |-- overall: double (nullable = true)
//    |-- reviewText: string (nullable = true)
//    |-- reviewTime: string (nullable = true)
//    |-- reviewerID: string (nullable = true)
//    |-- reviewerName: string (nullable = true)
//    |-- summary: string (nullable = true)
//    |-- unixReviewTime: long (nullable = true)

    reviewsRawDf.show(false)
    reviewsRawDf = reviewsRawDf.withColumn("order_id", col("_id").getItem("$oid"))
//    reviewsRawDf.show(5,false)

    reviewsRawDf = reviewsRawDf.withColumn("notHelpful",col("helpful")(0)).withColumn("actuallyHelpful",col("helpful")(1))
//    reviewsRawDf.show(5,false)

    reviewsRawDf = reviewsRawDf.withColumnRenamed("class","isSpam")
    reviewsRawDf = reviewsRawDf.drop("_id","asin","helpful","reviewText")

    reviewsRawDf.show(1,false)

//    reviewsRawDf.printSchema()

    reviewsRawDf.createOrReplaceTempView("reviews")

    val topSpammers = spark.sql("select reviewerID,reviewerName,count(order_id) as cnt from reviews where " +
      "isSpam = 1.0 group by reviewerID,reviewerName order by cnt desc limit 5")
    //shuffling happens 200 tasks are run.

    topSpammers.show(5,false)

    //can use gzip compression.or write as csv format with bzip2.  by default is snappy for parquet files.
//    topSpammers.coalesce(1).write
//      .format("csv").option("header",true).option("path","C:/TrendyTech/bigDataset").mode(SaveMode.Append).save

//    scala.io.StdIn.readLine()
  }
}
