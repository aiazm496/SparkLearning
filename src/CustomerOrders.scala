import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrders {
  def main(args: Array[String]): Unit = { //driver program

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","CustomerSpent")

    val rdd1 = sc.textFile("C:/TrendyTech/week9_downloads/customerorders-201008-180523.csv")
    //RDD[String] = RDD("44,8602,37.19","35,5368,65.89")
    // reads each line as string

    val rdd1ArrayOfEntries = rdd1.map(x=>x.split(","))
      .map(x=>(x(0),x(2).toDouble))// rdd(Array(string,int)) -> RDD(tuple(44,37.19), tuple(35,65.89))
      .reduceByKey((x,y)=>x+y)  //aggregrated key and value rdd(tuple(string,int))
      .sortBy(x=>x._2,ascending = false)

    rdd1ArrayOfEntries.collect().foreach(println) //collects all rdd data in driver node

    scala.io.StdIn.readLine()
  }
}
