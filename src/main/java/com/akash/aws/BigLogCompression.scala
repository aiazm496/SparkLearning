package com.akash.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

//read biglog.txt, write it as gzip file. then read gzip file using spark

object BigLogCompression extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("BigLog").master("local[*]").getOrCreate()

  val rdd = spark.sparkContext.textFile("C:/TrendyTech/week18_downloads/bigLogNew.txt").coalesce(16)

  val keyValueRdd = rdd.map(log => (log.split(": ")(0),
    log.split(": ")(1).split(" ")(5)))
  //RDD[("ERROR","2015")]

  import spark.implicits._

  val df = keyValueRdd.toDF("type","year")

  df.show(2)
  df.coalesce(1).write.format("csv").option("compression","bzip2")
    .option("path","C:/TrendyTech/week18_downloads/bigLogBzip2CompressedCSV ").mode(SaveMode.Overwrite)
    .partitionBy("year")
    .save()
   //  part-00000-47f23c8d-5dc0-4073-b7a2-d5a977d3db68-c000.csv.gz file created.

  //gzip files are not splittable, splittable for parquet, good compression
  //bzip2 files are splittable but take a lot of cpu power to compress. very good compression, can only be written as csv format not as parquet.
  //snappy is splittable for parquet. it is fastest, but least compression.

//    val df  = spark.read.format("csv")
//      .option("path","C:/TrendyTech/week18_downloads/bigLogGzipCompressed/part-00000-d2e093ba-0771-4b4e-be7a-e38f9d52590b-c000.csv.bz2")
//      .option("header",true).load
//
//    df.show(2)
//  println(df.rdd.getNumPartitions)

}
