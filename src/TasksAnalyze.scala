import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TasksAnalyze {
  def main(args: Array[String]): Unit = {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","taskAnalyze")
  val inputrdd = sc.textFile("C:/TrendyTech/week10_downloads/ratings-201014-183159.dat")
                    .map(x=>(x.split("::")(2),1)) //pair rdd

    val result = inputrdd.reduceByKey((x,y)=>x+y).sortBy(x=>x._2)

    result.saveAsTextFile("C:/TrendyTech/week10_downloads/Ratings_output")


  }
}
