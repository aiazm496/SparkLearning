import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregation {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().
      config(sparkConfig).getOrCreate()

    val orderDf = spark.read.format("csv")
                  .option("inferSchema",true)
                  .option("header",true)
                  .option("path","C:/TrendyTech/week12_downloads/order_data.csv").load

    orderDf.show(2,false)

    //show total rows,total quantity, avg price, distinct count of invoice no.
    //using org.apache.spark.sql.functions._
    //simple aggregation (no grouping) only 1 row output
    orderDf.select(count("*").as("Row Count"),
      sum("Quantity").as("Total Quantity"),
      avg("UnitPrice").as("Avg price"),
      countDistinct("InvoiceNo").as("Count Distinct Invoices")
    ).show()

    //with selectExpr we can use sql like syntax using string expression
    orderDf.selectExpr("count(*) as RowCount","sum(Quantity) as TotalQuantity",
                      "avg(UnitPrice) as Avg_price",
                      "count(distinct(InvoiceNo)) as Count_Distinct_Invoices ").show(false)

    //using spark sql way
    orderDf.createOrReplaceTempView("orders")
    val orderDfAgg = spark.sql("select count(*) as RowCount,sum(Quantity) as" +
      " TotalQuantity,avg(UnitPrice)" +
      " as Avg_price,count(distinct(InvoiceNo)) as Count_Distinct_Invoices from orders")

    orderDfAgg.show()

  }
}
