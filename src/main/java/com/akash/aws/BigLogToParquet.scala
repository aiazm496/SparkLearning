package com.akash.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object BigLogToParquet extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("BigLog").master("local[*]") //onlocal driver/executor is same.
    .config("spark.sql.shuffle.partitions",3)
//    .config("spark.executor.memory","12G")
    .getOrCreate()

  //filedata -> ERROR: Thu Jun 04 10:37:51 BST 2015
  val bigLogRdd = spark.sparkContext.textFile("C:/TrendyTech/week18_downloads/bigLogNew.txt")
    .map(x=>(x.split(": ")(0),x.split(": ")(1).trim.split(" ")(5))).coalesce(16)
  //RDD[("ERROR","2015")]  //we changed to 16 partitions from 44 default so that all 16 cores of local machine can run parallely as they
  //will launch at the same time.

//  println(bigLogRdd.getNumPartitions) //16

  import spark.implicits._
  val bigLogDf = bigLogRdd.toDF("type","year")
//  println(bigLogDf.rdd.getNumPartitions) //16

//  bigLogDf.show(2)

  bigLogDf.write.option("path","C:/TrendyTech/week18_downloads/bigLogParquet").partitionBy("year").mode(SaveMode.Overwrite).save
  scala.io.StdIn.readLine()
  spark.stop()
}

//Impact
//athena charges to read 1tb data is 5$ ~ 400rs
//1.5gb it will be 400/1000 = 0.4 + 0.2 = 0.6rs
//if the file is coming daily so for a year => 0.6*365 = 219 rs ~ 3$
//
//5mb , 400*5/1000*1000   = 1/2000 = 0.002 rs
//if the file is coming daily so for a year => 0.002 * 365  = 0.73 rs ~ 0$
//
////we have cut the cost by a lot on AWS when using services like S3(for storage), Athena(for querying data).
