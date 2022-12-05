package com.akash.aws

import org.apache.spark.SparkContext

object WordCount {

    def main(args: Array[String]): Unit = {

        val sc = new SparkContext() //master will be ec2 instance

        val rdd = sc.textFile("s3n://akash-aws-learning/samplewords.txt")
        //bucket name = akash-aws-learning

        val rdd2 = rdd.flatMap(x => x.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _).sortBy(_._2, false)

        val arr = rdd2.collect() //action

        arr.foreach(println)
    }
}
