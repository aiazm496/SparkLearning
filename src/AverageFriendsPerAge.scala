import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AverageFriendsPerAge {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","AverageFriendsPerAge")

    val input = sc.textFile("C:/TrendyTech/week9_downloads/friendsdata.csv")

    val result = input.map(x=>x.split("::").slice(2,4))  //RDD[Array("33","300")]
      .map(x=>(x(0),(x(1).toInt,1)))  // RDD[("33",(300,1)]
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)) //RDD[("33",(600,2))] //in reduce by key two values for a key is sent
      // x is 1 value and y is another value of same key but different
      .mapValues(x=> x._1/x._2) //x is tuple which is value
    //RDD[(String,Int)] // RDD[("33",385(average))]
      .sortBy(x=>x._2,ascending = false)
    //transformation ends

    //do action and collect on local
    result.collect().foreach(println)


    //output

  }
}
