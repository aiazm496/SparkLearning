import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateLessSparkWordCountStreaming extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","stateless")

  //creating spark streaming context, every 5 seconds a new RDD is created.
  val ssc = new StreamingContext(sc,Seconds(5))

  //lines is a DStream
  val lines = ssc.socketTextStream("localhost",9990)
  //DStream of lines

  val words = lines.flatMap(_.split(" "))

  val pairs = words.map((_,1))

  val wordsCount = pairs.reduceByKey(_+_)

  wordsCount.print()  //only display on batch (1 rdd sum)

  ssc.start()

  ssc.awaitTermination() //required else streaming app will stop

}
