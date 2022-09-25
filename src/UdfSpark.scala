import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object UdfSpark{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.master","local[*]")
    sparkConfig.set("spark.app.name","rough")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val ordersDf = spark.read.format("csv")
      .option("path","C:/TrendyTech/week12_downloads/orders.csv")
      .option("header",true)
      .load

     def isEven(id:Int) = if(id%2==0) "Even" else "Odd"

    spark.udf.register("isEven",isEven(_:Int))

    val newOrdersDf = ordersDf.withColumn("isEven",
      expr("isEven(order_customer_id)"))

    newOrdersDf.show(5)

  }
}

