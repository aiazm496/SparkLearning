import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Parallelize {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //creata list in scala and load it in RDD
    //count number of times warn and error has come

    val myList = List("WARN: Tuesday 4th September 2005",
                      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408",
      "ERROR: Tuesday 4th September 0408")

    val sc  = new SparkContext("local[*]","Parallelize list")

    val rdd = sc.parallelize(myList)
    //[Rdd(String)]

    //RDD[(string,int)] this is called as pair rdd.
    val result = rdd.map(x=>(x.split(":")(0),1)).reduceByKey((x,y)=>x+y)

    result.collect().foreach(println)

  }
}
