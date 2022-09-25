import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object DataframeRough {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name","Roughdf")
    sparkConfig.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val ordersDf = spark.read.format("csv")
      .option("path","C:/TrendyTech/week11_downloads/orders.csv")
      .option("header",true)
      .option("inferSchema",true)
      .load()

    ordersDf.printSchema()
    ordersDf.show(3)
    val closedOrders = ordersDf
                      .where("order_status = 'CLOSED'")
                      .select("order_id","order_status")
    closedOrders.show(3)

    val schemaOrdersDf = "order_id Int, order_date Timestamp, order_customer_id Int," +
      "order_status String" //scala data types

    val ordersDfManualSchema = spark.read.format("csv")
      .option("path","C:/TrendyTech/week11_downloads/orders.csv")
      .option("header",true)
      .schema(schemaOrdersDf)
      .load()

    ordersDfManualSchema.printSchema()
    ordersDfManualSchema.show(2)

    //working with json
    //schema already inferred.

    val jsonDf = spark.read.format("json")
      .option("path","C:/TrendyTech/week11_downloads/players.json")
      .load()

    jsonDf.printSchema()
    jsonDf.show()


    val windowSchema = "country String, x1 Int, x2 Int, x3 Int, x4 Double"

    val windowDf = spark.read.format("csv")
      .option("path","C:/TrendyTech/week11_downloads/windowdata.csv")
      .schema(windowSchema)
      .load()

    windowDf.show(1)
    windowDf.printSchema()

  }
}
