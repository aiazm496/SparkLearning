import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Aggregation3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name", "schemaExplicit")
    sparkConfig.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val windowDf = spark.read.format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .option("path","C:/TrendyTech/week12_downloads/windowdata_with_header.csv").load
    //country,weeknum,numinvoices,totalquantity,invoicevalue
    //Spain,49,1,67,174.72
    //Spain,50,2,400,1049.01
    //Spain,48,1,400,620.0

    //derive the rank based on country's  total invoice value


    windowDf.createOrReplaceTempView("window")
    val windowDfSumInvoiceByCountryDf = spark.sql("select country, " +
      "sum(invoicevalue) as totalInvoice " +
      "from window group by country").sort("totalInvoice")

    windowDfSumInvoiceByCountryDf.show()

//    val window = Window.orderBy("totalInvoice") gives 1st rank to lowest totalInvoice

    val window = Window.orderBy(col("totalInvoice").desc)
    //using column string we can modify column that is an advantage

    //add rank column
    val windowDfRankedBasedOnTotalInvoiceDf = windowDfSumInvoiceByCountryDf
                                              .withColumn("rankBasedOnTotalInvoice",
                                                rank().over(window))
    windowDfRankedBasedOnTotalInvoiceDf.show(5)


  }
}
