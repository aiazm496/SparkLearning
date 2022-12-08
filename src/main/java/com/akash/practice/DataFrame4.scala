package com.akash.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame4 {

  case class Data(id:Int, name:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("rough").master("local[*]").getOrCreate()

    val data = List((1,"Akash"),(2,"Rohan"),(3,"Amit"))

    val winners = List("Akash","Amit")

    val winnerBroadcast = spark.sparkContext.broadcast(winners)

    import spark.implicits._

    val df = data.toDF("id","name")

    df.show()

    val ds = df.as[Data]

    print(ds.rdd.getNumPartitions) //3 partitions so 3 task so 3 executors run parallely, no idea of winners so broadcast.

    ds.filter(data => winnerBroadcast.value.contains(data.name)).show()

    df.withColumn("fullname",expr("concat(name,'ji')")).show()
    

  }

}
