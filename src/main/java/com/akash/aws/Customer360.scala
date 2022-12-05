package com.akash.aws

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object Customer360 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val ordersDf = spark.read.format("csv").option("header",true).option("inferSchema",true)
      .option("path","airflow_input/orders.csv").load

    val closedOrdersDf = ordersDf.filter("order_status = 'CLOSED'")

    closedOrdersDf.write.format("csv").option("path","airflow_output").mode(SaveMode.Overwrite).save() //airflow_output

    return 0

  }

}
