package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Aggregation2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val windowDf = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("path", "C:/TrendyTech/week12_downloads/windowdata_with_header.csv").load

    //country,weeknum,numinvoices,totalquantity,invoicevalue
    //Spain,49,1,67,174.72
    //Spain,50,2,400,1049.01
    //Spain,48,1,400,620.0
    //result-> Spain, 48, 1,400, 620.0,    620.0(new col)
    //         Spain, 49, 1, 67, 174.72,   794.72(new col)
    //         Spain, 50, 2, 400, 1049.01, 1843.73(new col)

    val window = Window.partitionBy("country").orderBy("weeknum")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    //unboundedPreceding-> first row

    //add new running sum column runningInvoiceSum
    val myDf = windowDf.withColumn("runningInvoiceSum",
      sum(col("invoicevalue")).over(window))

    myDf.show()

  }
}
