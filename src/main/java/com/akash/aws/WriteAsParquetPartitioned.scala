package com.akash.aws

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WriteAsParquetPartitioned extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("rough").master("local[*]")
    .config("spark.sql.shuffle.partitions",3)
    .getOrCreate()

  val studentDf = spark.read.format("csv")
    .option("path","C:/TrendyTech/week18_downloads/students_with_header-201214-111236.csv")
    .option("header",true).option("inferSchema",true).load

  println(studentDf.printSchema())
  studentDf.show(5)

  //by default spark writes in parquet
  studentDf.write.partitionBy("subject")
    .option("path","C:/TrendyTech/week18_downloads/students_partioned_by_subject_parquet").save

  scala.io.StdIn.readLine()
  spark.stop()

}
