package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Parallelism {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Save")

    println(sc.defaultParallelism) //tells default no of partitions created in rdd when
    //we use parallelize on collections  == 16
    println(sc.defaultMinPartitions) //which determine min no of partitions when we load from file.
    //==2

    val myList = List("WARN: Tuesday 4th September 2005",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408")

    val rdd = sc.parallelize(myList)
    println(rdd.getNumPartitions) //16
    //if we have a 4 node cluster then each node will have 4 partitions.
    //if we have an executor with 16 cores then all tasks can be run parallely.

    val incRdd = rdd.repartition(32) //increase number of partitions
    //    //it is a wide transformation so shuffling is required.
    //    println(incRdd.getNumPartitions) //32
    //
    //    val resultRdd = incRdd.map(x=>x)     //32 tasks have run as no of partitions
    //    //so increase partitions to achieve more parallelism
    //    // we should increase partition before any transformation where we feel each
    //    //partition will have huge data, so it avoid a huge data task/partition
    //
    //    val filterRdd = resultRdd.filter(x=>x=="WARN: Tuesday 4th September 2005")
    //    //filterdRdd data is collection of 32 partitions( out of which mostly many will be empty)
    //
    //    //do some transformation on filterRDD
    //    val ac = filterRdd.map(x=>x)
    //
    //    //we can create new rdd with reducing partitions after filter transformation when
    //    //we see that after filter very less data is remaining, so no point in keeping many
    //    //partitions as empty and processing those.
    //
    //    val colRdd = rdd.coalesce(2)
    //    //coalesce can be used only to reduce partitions
    //    //coalesce is a wide transformation but combines existing partitions on same machine
    //    //to avoid full shuffle like repartition. hence it is preferred when we need to decrease
    //    //the no of partitions.
    //
    //    resultRdd.collect().foreach(println)
    //    ac.collect().foreach(println)
    //any transformation/action triggers tasks in rdd on partitions.
    rdd.collect().foreach(println) //16 tasks run to parallelize
    incRdd.collect().foreach(println) //16 tasks run in stage1 for repartitioning of rdd
    //32 tasks run in parallel to collect

    scala.io.StdIn.readLine();

  }
}
