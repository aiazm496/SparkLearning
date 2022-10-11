package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
//save data as table in hive
object DataFrameWrite_part2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "schemaExplicit")
  sparkConfig.set("spark.master", "local[*]")

  val spark = SparkSession.builder().
    config(sparkConfig).
    enableHiveSupport() //enable hive support to persist(store metadata as per hive permanently)
    //else the metadata is stored in memory and is deleted after spark session ends.
    .getOrCreate()

  val ordersDf = spark.read.format("csv")
    .option("path", "C:/TrendyTech/week12_downloads/orders.csv")
    .option("inferSchema", true)
    .option("header", true).load()

  //write as table in a retail db and store metadata(schema) in hive metastore(derby db)
  //metastore_db folder we create schema in metastore for permanent storage

  //create db supported by hive environment
  spark.sql("create database if not exists retail")

  //like hive saved in spark-warehouse/retail.db/orders dir
  ordersDf.write.format("csv").mode(SaveMode.Overwrite)
    .bucketBy(4, "order_id") //save in buckets using hash function
    .sortBy("order_id") //order_id cardinality is high so used bucketing and sort for optimization
    .saveAsTable("retail.Orders")

  spark.catalog.listTables("retail").show() //list all db tables

  spark.stop()

}
