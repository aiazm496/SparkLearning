import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Ratings {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","ratings-movies")

    //filter movies with more than 4.5 star avg rating and atleast 1000 people have rated for the movie

    //dataset movies.dat contains movies.data contains movieid and title name
    //dataset ratings.data contains userId,movieId,rating,unixtimestamp

    val ratingsRdd = sc.textFile("C:/TrendyTech/week11_downloads/ratings.dat")

    val mappedRdd = ratingsRdd.map(x=> {
      val fields = x.split("::")
      (fields(1),fields(2).toFloat)
    })
    //RDD[(movieId,rating)]
    //RDD[("1",4.0)]
    //find avg rating

    val step1RddAvgRating = mappedRdd.mapValues(x=>(x,1))
    //RDD[(movieId,(rating,1))]
    // RDD[("1",(4.0,1))]
    // RDD[("1",(3.0,1))]

    val avgRatingRDD1 = step1RddAvgRating.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
    //RDD[("1",(7.0,2))]    movieid, (totalRating, count of rating)

    //filter out ratings count > 1000
    val avgRatingRDD2 = avgRatingRDD1.filter(x=> x._2._2>1000)
    //RDD[("1",(10000.0,2000))]

    //find average rating

    val avgRatingRDD3= avgRatingRDD2.mapValues(x=> x._1/x._2)
    //RDD[("1", 5.0)]

    val filterRDD = avgRatingRDD3.filter(x=> x._2>=4.5)

    val moviesRDD = sc.textFile("C:/TrendyTech/week11_downloads/movies.dat")
      .map(x=> {
          val fields = x.split("::")
        (fields(0),fields(1))
      })
    //RDD[(movieId,titleName)]

    val finalRdd1 = moviesRDD.join(filterRDD)
    //RDD[(movieId,(titleName,Rating)]

    val finalRdd2 = finalRdd1.map(x=>(x._2._1,x._2._2)).sortBy(x=>x._2,ascending = false)

    finalRdd2.collect().foreach(println)

  }
}
