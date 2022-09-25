import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object DataFrameExample2 {

  case class OrdersData(order_id:Int, order_date: Timestamp, order_customer:Int, order_status: String)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","df1")
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //instead of rdd, we create spark dataframe
    val ordersDf = spark.read.option("header",true).option("inferSchema",true)
      .csv("C:/TrendyTech/week11_downloads/orders.csv")
    //read is action //header true will make first row in csv as header.

    ordersDf.filter("orders_id > 100") //there is no column with name orders_id
    //but it will give error not at compile time but at run time.

    //for checking error at compile time we need to convert the dataframe to dataset.
    import spark.implicits._

    val ordersDs = ordersDf.as[OrdersData]
//    ordersDs.filter(x=>x.order_ids > 10).show() gives error at compile time can't resolve order_ids

    scala.io.StdIn.readLine()
    spark.stop()

  }
}
