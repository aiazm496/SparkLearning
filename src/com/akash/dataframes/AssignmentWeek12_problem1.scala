package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object AssignmentWeek12_problem1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.master", "local[*]")
    sparkConfig.set("spark.app.name", "week12_problem1")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val empDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week12_downloads/assignment/employee.json")
      .load

    val deptDf = spark.read.format("json")
      .option("path", "C:/TrendyTech/week12_downloads/assignment/dept.json")
      .load

    empDf.show(2, false)
    deptDf.show(2, false)

    //display deptName,deptid,countOfEmployees
    //IT,11,1
    //HR,21,1

    val joinEmpDeptDf = empDf.join(deptDf,
      empDf.col("deptid") === deptDf.col("deptid"), "right")
      .drop(empDf.col("deptid"))

    joinEmpDeptDf.show(10, false)

    joinEmpDeptDf.createOrReplaceTempView("joinEmpDept")

    //    val resultDf = joinEmpDeptDf.groupBy("deptName","deptid").count("id")

    val resultDf = spark.sql(
      """select deptName,deptid, count(id) from joinEmpDept
        |group by deptName,deptid""".stripMargin)

    resultDf.show()

    spark.stop()
  }

}
