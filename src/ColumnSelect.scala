import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ColumnSelect extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name","schemaExplicit")
  sparkConfig.set("spark.master","local[*]")

  val spark = SparkSession.builder().
    config(sparkConfig).getOrCreate()

  val ordersDf = spark.read.format("csv")
    .option("path","C:/TrendyTech/week12_downloads/orders.csv")
    .option("inferSchema",true)
    .option("header",true).load()

  ordersDf.select("order_id","order_status").show(3)
//  ordersDf.select("order_id","concat(order_status,'_status')").show(3)
  //cant use column string and column expression together
  ordersDf.selectExpr("order_id","concat(order_status,'_status')").show(3,false)

  spark.stop()

}
