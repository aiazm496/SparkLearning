import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ProcessingUnstructuredFile {

  case class Orders(order_id:Int, customer_id:Int, order_status:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //1 2013-07-25	11599,CLOSED
    //2 2013-07-25	256,PENDING_PAYMENT
    //this can't be loaded as df as it has no comman sep
    //convert it to rdd use map transformation, for each line create a case class object
    //as case class object will be having data and schema(fields), we can convert the new rdd
    //to dataset rdd.toDs() and perform high level constructs

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.app.name","RddToDs")
    sparkConfig.set("spark.master","local[2]")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    val rdd = spark.sparkContext.textFile("C:/TrendyTech/week12_downloads/orders_new.csv")
    //rdd[string] -> "1 2013-07-25	11599,CLOSED"

    val regex = """^(\S+) (\S+)\t(\S+),(\S+)""".r //^ indicates it is start of pattern
    //() use this to parameterize

    //this will return Orders case class object for each line that is passed.
    def parser(line:String) = { //line is "1 2013-07-25	11599,CLOSED" when we use map on rdd
      line match {
        //we captured various fields in regex as we use () in regex
        case regex(order_id,date,customer_id,order_status) =>
          Orders(order_id.toInt,customer_id.toInt,order_status) //returns case class object
      }
    }

    val rdd2 = rdd.map(parser) //Rdd[Orders] rdd of Orders

    import spark.implicits._

    val ordersDs = rdd2.toDS()

    ordersDs.show(1)

    ordersDs.groupBy("order_status").count().show()

  }
}
