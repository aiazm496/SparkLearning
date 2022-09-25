import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Assignment2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Assignment-2")

    val input = sc.textFile("C:/TrendyTech/week9_downloads/tempdata.csv")
    //Rdd["ITE00100554,18000101,TMAX,-75,,,E,..."]

    val inputNeeded = input.map(x=> {
        val array = x.split(",")
        val id = array(0)
        val temp = array(3)
      (id,temp.toInt)
    })
    //Rdd[("ITE00100554",-75)]

    val minTempForId = inputNeeded.reduceByKey((x,y)=>Math.min(x,y))

    minTempForId.collect().foreach(println) //action and stores in local

  }
}
