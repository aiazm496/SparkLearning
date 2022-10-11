package com.akash.dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AssignmentWeek12_problem2 {

  case class Movies(movieId: Int, movieName: String, genre: String)

  case class Ratings(userId: Int, movieId: Int, rating: Int, timestamp: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.master", "local[*]")
    sparkConfig.set("spark.app.name", "week12_problem1")

    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()

    //    val empDf = spark.read.format("csv")
    //      .option("delimiter","::")
    //      .option("path","C:/TrendyTech/week11_downloads/movies.dat")
    //      .load
    // we can't create dataframe out of file movies.data as it has delimiter :: which is more than
    //one character.
    // so create rdd then convert to df

    val rddMovies = spark.sparkContext.textFile("C:/TrendyTech/week11_downloads/movies.dat")
    //Rdd[String]
    //    rddMovies.collect().foreach(println)

    val rddMoviesMap = rddMovies.map(x => {
      val array = x.split("::")
      (array(0).toInt, array(1), array(2))
    }) //Rdd[(1,"Toy Story (1995)","Animation|Children's|Comedy")]

    val rddMoviesMapCaseClass = rddMoviesMap.map(x => {
      Movies(x._1, x._2, x._3)
    }) //Rdd[Movies]  this rdd has schema now

    //convert to DataFrame
    import spark.implicits._

    val moviesDf = rddMoviesMapCaseClass.toDF()
    moviesDf.show(4, false)

    val ratingsRdd = spark.sparkContext.textFile("C:/TrendyTech/week11_downloads/ratings.dat")
    val ratingsRddMap = ratingsRdd.map(x => {
      val array = x.split("::")
      (array(0).toInt, array(1).toInt, array(2).toInt, array(3))
    }) //Rdd[(1,1193,5,978300760)]

    val ratingsRddCaseClass = ratingsRddMap.map(x => {
      Ratings(x._1, x._2, x._3, x._4)
    })

    val ratingsDf = ratingsRddCaseClass.toDF()
    ratingsDf.show(4, false)

    //find top movie by rating, moview with ratings more than 1000 times and avg rating > 4.5
    //ratings df -> |userId|movieId|rating|timestamp
    //movieDf  -> |movieId|movieName|genre

    //let's create df with only movieid,moviename and rating
    val movieRatingJoinDf = ratingsDf.join(broadcast(moviesDf),
      moviesDf.col("movieId") === ratingsDf.col("movieId"), "inner")
      .drop(ratingsDf.col("movieId"))
      .select(moviesDf.col("movieName"), moviesDf.col("movieId"),
        ratingsDf.col("rating"))

    movieRatingJoinDf.show(5, false)

    movieRatingJoinDf.createOrReplaceTempView("mr")
    val movieRatingPreDf = spark.sql("select movieId,movieName, avg(rating) as average," +
      "count(rating) as cnt from mr group by movieId, movieName ").filter(
      "cnt > 1000 and average > 4.5"
    ).sort(col("average").desc)

    movieRatingPreDf.show(5, false)

    scala.io.StdIn.readLine()

    spark.stop()
  }
}
