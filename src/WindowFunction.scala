import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}

object Rough {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.master","local[*]")
    sparkConfig.set("spark.app.name","Rough")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val orderDf = spark.read.format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .option("path","C:/TrendyTech/week12_downloads/order_data.csv").load

    orderDf.printSchema()
    orderDf.show(10,false)

    val filteredOrderDf = orderDf.filter("country = 'United Kingdom'")
      .filter("CustomerId = 14688").sort("InvoiceNo").limit(10)

    val window = Window.orderBy("InvoiceNo")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    filteredOrderDf.show()
    filteredOrderDf.withColumn("sumOfQuantity",
      sum(col("Quantity")).over(window)).show()

    spark.stop()

  }
}


