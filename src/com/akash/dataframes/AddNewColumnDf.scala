package com.akash.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object AddNewColumnDf {
  case class Person(name: String, age: Int, city: String)

  def ageCheck(age: Int) = if (age > 18) "Y" else "N"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().
      config(sparkConfig).getOrCreate()

    val df = spark.read.format("csv")
      .option("path", "C:/TrendyTech/week12_downloads/dataset1.dataset1")
      .option("inferSchema", true).load()

    df.printSchema() //  ordersDf.printSchema()

    val df1 = df.toDF("name", "age", "city") //add columns
    df1.printSchema()
    df1.show(2)
    //
    //  import spark.implicits._
    //  val ds1 = df1.as[Person] //dataset
    ////  ds1.filter(x=>x.age>30) we can use anonymous func in datasets not in df.
    //
    //  val df2 = ds1.toDF() //dataset to dataframe
    //  df2.printSchema()

    //add new column, if age > 18, then Y else N
    //    val parseAgeFunction  = udf(ageCheck(_:Int))
    //
    //    val df2 = df1.withColumn("adult",parseAgeFunction(col("age")))
    //    //new col adult added created using udf
    //    df2.show()
    //with above approach we didn't register the udf so it will not be available in spark catalog
    //or across spark sql

    //registering udf in spark
    spark.udf.register("parseAgeFunction", ageCheck(_: Int))
    val df2 = df1.withColumn("adult", expr("parseAgeFunction(age)"))
    df2.show()

    //as parseAgeFunction function is registered we can use it
    df1.createOrReplaceTempView("dataframe1")
    val df3 = spark.sql("select name,age,city,parseAgeFunction(age) as adult from dataframe1")
    df3.show()

    val df4 = df1.withColumn("city_upper", expr("upper(city)"))
    df4.show()

    spark.stop()
  }
}
