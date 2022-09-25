import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object CustomerOrdersOptimize {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","CustomerOrders")

    val rdd = sc.textFile("C:/TrendyTech/week9_downloads/customerorders.csv").map(x=>  {
                          val details = x.split(",")
      (details(0).toInt,details(1).toFloat)
      })
    //Rdd[(customerid,orderValue)]

    val totalOrderPerCustomer = rdd.reduceByKey((x,y)=>x+y)
    //filter premium customer , order value > 5000

    val premiumCustomers = totalOrderPerCustomer.filter(x=> x._2>5000)
    //RDD[(customerid,orderValue)]

//    val doubledAmount  = premiumCustomers.map(x=>(x._1,x._2*2)).cache()
    //RDD[(customerid,orderValue)]
    //as we have cached this rdd, so when action-2 runs, then it directly gets the doubledAmount rdd from memory
    //as it cached. It is stored in non-serialized format in memory.(non-serialized is fast as it processed faster but takes more space
    // as serialized is stored in bytes but takes more processing time as it is in bytes.
    //if not cached then all above rdds are recalculated.

    val doubledAmount  = premiumCustomers.map(x=>(x._1,x._2*2)).persist(StorageLevel.DISK_ONLY_2)
    // most preferred form of storage, if memory is full, it stores in disk. persist(StorageLevel.MEMORY_AND_DISK)
    //in disk it is in serialized format(bytes) takes less space.
    //StorageLevel.DISK_ONLY_2 stores in deserialized format in disk with 2 replicas, so fault tolerance is taken care with 2 worker nodes.

    println(doubledAmount.toDebugString)  //used to see lineage graph of an rdd

    //.persist(StorageLevel.DISK_ONLY_2)
    //(2) MapPartitionsRDD[5] at map at CustomerOrdersOptimize.scala:31 [Disk Serialized 2x Replicated]

    doubledAmount.collect().foreach(println) //action-1

    println(doubledAmount.count()) //action-2

    scala.io.StdIn.readLine()

  }
}
