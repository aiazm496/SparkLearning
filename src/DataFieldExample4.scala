import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import java.sql.Timestamp

object DataFieldExample4 {

  case class Orders(order_id:Int, order_date:Timestamp, order_customer_id:Int, order_status:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name","schemaExplicit")
    sparkConfig.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    //in spark the types are scala type + Type
//    val ordersSchema = StructType(List(StructField("order_id",IntegerType,false), //false means nullable false
//                                  StructField("order_date",TimestampType,false),
//                                  StructField("order_customer_id",IntegerType),
//                                  StructField("order_status",StringType)))

    //using ddl way to explicitly define schema of dataframe
    val ddlOrdersSchema = "order_id Int, order_date Timestamp , order_customer_id Int, order_status String"

    val ordersDf = spark.read.format("csv")
                   .option("header",true)
                   .option("path","C:/TrendyTech/week11_downloads/orders.csv")
                   .schema(ddlOrdersSchema)
                   .load()

    ordersDf.printSchema()
    ordersDf.show()

    //converting dataframe to dataset
    //first create case class
    import spark.implicits._

    val ordersDs = ordersDf.as[Orders]
    ordersDs.filter("order_id > 1000").show()
    ordersDs.filter(x=> x.order_id> 1000).show()
    ordersDs.printSchema()

    spark.stop()

  }

}
