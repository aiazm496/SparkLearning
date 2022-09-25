import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.DoubleType

object AdvantageUsingCol extends App{

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

    //select country only spain and weeknum desc, invoice value
    //change datatype on the fly while selecting column using col method

  windowDf.select(col("country"),
    col("weeknum").cast(DoubleType),col("invoicevalue")).show(2)

  windowDf.selectExpr("country","upper(weeknum) as WEEKNUM","invoicevalue").show(2)

}
