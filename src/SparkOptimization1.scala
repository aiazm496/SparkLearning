import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SparkOptimization1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","optimization_1")

    val range = 1 to 100

    val rdd = sc.parallelize(range)
    //didn't observe in dag or execution that spark is filtering first then map.
    //didn't observe that spark is only loading x %2 amd x%3==0 in map as told by sir.
    val rdd2 = rdd.map(x=>{print(x + " ");x})

    val rdd3 = rdd2.filter(x=>x%2==0 && x%3==0)

    rdd3.collect().foreach(println)

    scala.io.StdIn.readLine()

  }
}
