package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExample3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "dataframeExample3")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()

    val ordersDf = spark.read
      .format("csv")
      .option("header", true)
      .option("path", "C:/TrendyTech/week11_downloads/orders.csv")
      .load()


    ordersDf.show()
    ordersDf.printSchema() //it guesses schema from data

    val filteredOrdersDf = ordersDf
      .filter("order_customer_id > 10000")

    filteredOrdersDf.show()

    //loading file with each record(line) is a json
    val playersDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week11_downloads/players.json").load()
    // schema is already defined in json

    playersDf.printSchema()
    playersDf.show()

    //loading file with 2 lines having incorrect json
    val playersDf2 = spark.read.format("json").option("path", "C:/TrendyTech/week11_downloads/players_error.json").load()
    // we have 3 option modes in dealing with corrupt record  for this json , by default it is permissive which displays corrupt record.
    //2nd is DROPMALFORMED which drops corrupted record and last is FAILFAST which throws exception if there is a corrupt record.

    playersDf2.printSchema()
    playersDf2.show(false) //false to remove truncation of columns

    val playersDf3 = spark.read.format("json")
      .option("path", "C:/TrendyTech/week11_downloads/players_error.json").
      option("mode", "DROPMALFORMED").load()

    playersDf3.printSchema()
    playersDf3.show(false) //false to remove truncation of columns

    //IF WE DON'T specify format, it reads as parquet file

    val usersParquetDf = spark.read.option("path", "C:/TrendyTech/week11_downloads/users.parquet").load()
    //schema is already defined in parquet.

    usersParquetDf.printSchema()
    usersParquetDf.show()

  }
}
