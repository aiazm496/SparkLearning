package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object JoinPractice2 {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]")
      .getOrCreate()


    val empDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week12_downloads/assignment/employee.json")
      .load

    val deptDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week12_downloads/assignment/dept.json")
      .load

    empDf.show(2)
    deptDf.show(2)

    empDf.createOrReplaceTempView("employee")
    deptDf.createOrReplaceTempView("dept")

//    val joinedDf = empDf.join(deptDf,expr("employee.deptid = dept.deptid"))

    val joinedDf = spark.sql("select e.deptid,d.deptName, count(e.id) as count from employee e join dept d on e.deptid = d.deptid group by e.deptid,d.deptName ")

    println(joinedDf.rdd.getNumPartitions)

    joinedDf.show()


    joinedDf.filter("count > 1").show()
    scala.io.StdIn.readLine()


  }
}
