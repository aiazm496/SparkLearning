import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DfJoin {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().
      config(sparkConfig).getOrCreate()

    val ordersDf = spark.read.format("csv")
                  .option("path","C:/TrendyTech/week12_downloads/orders.csv")
                  .option("header",true).load

    ordersDf.show(2)

    val customersDf = spark.read.format("csv")
      .option("path","C:/TrendyTech/week12_downloads/customers.csv")
      .option("header",true).load

    customersDf.show(2)

    //simple join
    //inner
    ordersDf.join(customersDf,
      ordersDf.col("order_customer_id")=== customersDf.col("customer_id"),
    "inner").show(1)

    ordersDf.join(customersDf,
      ordersDf.col("order_customer_id")=== customersDf.col("customer_id"),
      "right").sort("customer_id").show(5)

    //    scala.io.StdIn.readLine()
    spark.stop()

  }

}
