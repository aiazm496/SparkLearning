import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source

object Assignment_week10 {

  def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      val sc = new SparkContext("local[*]","Rough")

      val chapterDataRDD = sc.textFile("C:/TrendyTech/week10_downloads/assignment_dataset/chapters.csv").
        map(x=> {
          val details = x.split(",")
          (details(0).toInt,details(1).toInt)
        }) //RDD[(chapterId,courseId)]

      val viewDataRDD  = sc.textFile("C:/TrendyTech/week10_downloads/assignment_dataset/views/*").
        map(x=> {
          val details = x.split(",")
          (details(0).toInt,details(1).toInt)
        }) //RDD[(userId,chapterId)]

    //Exercise1: Find Number of Chapters Per Course
      val chapterCountRdd = chapterDataRDD.map(x=>(x._2,1)).reduceByKey((x,y)=>x+y).sortBy(x=>x._2,ascending = false)

      //chapterCountRdd.collect().foreach(println)

    val viewDataDistinctRDD = viewDataRDD.distinct() //removed duplicate views

    val flippedviewDataRDD = viewDataDistinctRDD.map(x=>(x._2,x._1)) //make chapterid as key
    //RDD[(chapterId,userId)]
    //chapterDataRDD = RDD[(chapterId,courseId)]

    val joinedRdd = flippedviewDataRDD.join(chapterDataRDD) //join on key chapterId
    //RDD[(chapterId,(userId,courseId))]

    val pairRdd = joinedRdd.map(x=>((x._2._1,x._2._2),1))
    //RDD[((userId,courseId),1)]

    val userPerCourseViewRDD = pairRdd.reduceByKey((x,y)=>x+y)
    //RDD[((userId,courseId),totalChaptersRead)]

    val courseViewsCountRDD = userPerCourseViewRDD.map(x=>(x._1._2,x._2))
    //RDD[(courseId,totalChaptersRead)]

    //chapterCountRdd - RDD[(courseId,totalChaptersCount)]

    val  newJoinedRDD = courseViewsCountRDD.join(chapterCountRdd)
    //RDD[(courseId,(totalChaptersRead,totalChaptersCount))]

    val courseCompletionpercentRDD = newJoinedRDD.mapValues(x=> x._1.toDouble/x._2)
    //RDD[(courseId,%courseCompleted)]

    val formattedpercentageRDD = courseCompletionpercentRDD.mapValues(x=> f"$x%.5f".toDouble)
//    formattedpercentageRDD.collect().foreach(println)
    //RDD[(courseId,%courseCompleted)]

    val scoreRdd = formattedpercentageRDD.mapValues( x => {
          if(x>=0.9) 10
          else if(x >=0.5 && x<0.9) 4
          else if(x>=0.25 && x <0.5) 2
          else 0
    })
    //RDD[(courseId, score)]

    val totalScorePerCourseRDD = scoreRdd.reduceByKey((x,y)=>x+y)
//    totalScorePerCourseRDD.collect().foreach(println)
    //RDD[(courseId, totalScore)]

    //remove course id, put title
    val titlesDataRDD = sc.textFile("C:/TrendyTech/week10_downloads/assignment_dataset/titles.csv")
                            .map(x=>(x.split(",")(0).toInt,x.split(",")(1)))
    //RDD[(courseId, title)]

    //join
    val title_score_joinedRDD = totalScorePerCourseRDD.join(titlesDataRDD).map(x=>(x._2,x._1))

    title_score_joinedRDD.collect().foreach(println)

    sc.stop();
  }
}
