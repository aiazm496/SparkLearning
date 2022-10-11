package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

object AddNewColumnDf_2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().
      config(sparkConfig).getOrCreate()

    val myList = List((1, "2013-07-25", 11599, "CLOSED"), (2, "2014-07-25", 256, "PENDING_PAYMENT"),
      (3, "2013-07-25", 11599, "COMPLETE"), (4, "2019-07-25", 8827, "CLOSED"))

    val df = spark.createDataFrame(myList).toDF("orderid", "orderdate", "customerid", "status")

    //convert orderdate column to unix timestamp use withColumn
    //add newid col with incremental values
    //drop duplicates (orderdate,customerid)

    val newDf = df.withColumn("orderdate",
      unix_timestamp(col("orderdate").cast(DateType)))
      .withColumn("newid", monotonically_increasing_id)
      .dropDuplicates("orderdate", "customerid")
      .drop("orderid")
      .sort("orderdate")

    newDf.printSchema()
    newDf.show()


  }

}
