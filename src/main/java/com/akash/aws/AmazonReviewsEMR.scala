package com.akash.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

//write spark program to process json(2gb), create dataframe with reviewerID, reviewerName, and isSpam
//load the reviewerID, reviewerName, and isSpam in S3.
//use Athena to find the top 5 spammers by grouping on reviewerId.

//data is in s3, the spark program will run on EMR and it should put the reviewers data in s3.

object AmazonReviewsEMR {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("BigLog").getOrCreate()

    var reviewsRawDf = spark.read.format("json").option("path","s3://customer-reviews-big-data/Cell_Phones_and_Accessories.json").load

//    reviewsRawDf.printSchema()
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


//    reviewsRawDf.show(false)
    reviewsRawDf = reviewsRawDf.withColumn("order_id", col("_id").getItem("$oid"))
//    reviewsRawDf.show(5,false)

    reviewsRawDf = reviewsRawDf.withColumn("notHelpful",col("helpful")(0)).withColumn("actuallyHelpful",col("helpful")(1))
//    reviewsRawDf.show(5,false)

    reviewsRawDf = reviewsRawDf.withColumnRenamed("class","isSpam")
    reviewsRawDf = reviewsRawDf.drop("_id","asin","helpful","reviewText")

//    reviewsRawDf.show(1,false)

//    reviewsRawDf.printSchema()

//    reviewsRawDf.coalesce(1).write.option("path","C:/TrendyTech/bigDataset/parquetCompressed").option("compression","gzip").mode(SaveMode.Append).save()
    //copy the gzip to s3 bucket customer-reviews-gzip-parquet

    reviewsRawDf.createOrReplaceTempView("reviews")

    val topSpammers = spark.sql("select reviewerID,reviewerName,count(order_id) as cnt from reviews where " +
      "isSpam = 1.0 group by reviewerID,reviewerName order by cnt desc limit 5")

//    topSpammers.show()

    topSpammers.coalesce(1).write.format("csv").option("header",true).option("path","s3://customer-reviews-emr-output/").mode(SaveMode.Append).save

  }
}
