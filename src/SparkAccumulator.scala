import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SparkAccumulator {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Rough")

      //the txt has blank lines
    val inputRdd = sc.textFile("C:/TrendyTech/week10_downloads/samplewords.txt" )

    val myAccum = sc.longAccumulator("Blank lines accumulator")
    //accumulator is kept on driver machine and can be updated by executors on worker nodes

    inputRdd.foreach(x=> if(x=="") myAccum.add(1))
    //so we use accumulator to aggregate results from rdd tasks on cluster

  }
}
