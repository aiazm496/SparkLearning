import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object Assignment_week11_problem_1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name","p1_week11")
    sparkConfig.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    //have to give spark types ( scalatype + Type)
    val programmaticSchema = StructType(List(StructField("country",StringType),StructField("weeknum",IntegerType),
      StructField("numinvoices",IntegerType),StructField("totalquantity",IntegerType),StructField("invoicevalue",FloatType)))

    val windowDf = spark.read.format("csv")
                  .option("path","C:/TrendyTech/week11_downloads/windowdata.csv")
                  .schema(programmaticSchema).load

    windowDf.printSchema()
    windowDf.show()
//    windowDf.where("weeknum > 49").show()

    //write windowdf as parquet file and save in way that we have combo of country-weeknum.

    //by default it saves in parquet format
    windowDf.write.option("path","C:/TrendyTech/week11_downloads/problem1")
      .partitionBy("country","weeknum").mode(SaveMode.Overwrite).save()

    //write windowdf as avro file
//    windowDf.write.format("avro").option("path","C:/TrendyTech/week11_downloads/problem1_avro").save()
  }

}
