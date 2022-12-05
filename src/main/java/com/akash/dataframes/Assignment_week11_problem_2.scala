package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Assignment_week11_problem_2 {

  case class WindowData(country: String, weeknum: Int, numinvoices: Int, totalquantity: Int,
                        invoicevalue: Float)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "p2_week11")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val rdd = spark.sparkContext.textFile("C:/TrendyTech/week11_downloads/windowdata.csv")

    import spark.implicits._
    val windowDf = rdd.toDF()
    windowDf.show()

    windowDf.write.format("json")
      .option("path", "C:/TrendyTech/week11_downloads/p2_json").save()

  }
}
