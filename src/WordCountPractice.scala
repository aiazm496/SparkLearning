import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountPractice {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","Practice")

    val rdd1 = sc.textFile("C:/TrendyTech/week9_downloads/search.txt")

    val x =rdd1.flatMap(x=>x.split(" "))
      .map(x=>x.toLowerCase())
      .map(x=>(x,1))
      .reduceByKey((x,y)=>x+y)
      .sortBy(x=>x._2,ascending = false)

    x.collect().foreach(println)

//    for(i<- x) {
//      val a = i._1
//      val b = i._2
//      println(s"$a:$b")

    }
  }
