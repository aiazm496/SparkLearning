package com.akash.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Week10Assignment {
  def main(args: Array[String]): Unit = {

    //    Exercise 1:Find Out how many Chapters  are  there per course.

    //chapter.csv has data, chaptersheader.csv has header info(chapterId,courseId)
    //output want like courseId and count of chapters

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "assignment_week10")

    val rdd = sc.textFile("C:/TrendyTech/week10_downloads/assignment_dataset/chapters.csv")
    //rdd[String]  -> rdd["0,1"]

    val crdd = rdd.map(x => (x.split(",")(1), 1))
    //rdd[("courseid",1)]   pair rdd

    val totalChapterRdd = crdd.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false)
    //rdd[("courseid",totalchapters)]

    totalChapterRdd.collect().foreach(println)

    //views1.csv,views2.csv and views3.csv it has userId,chapterId, dateAndTime
    // we need to find total score of each course - > if user watched >= 90%course then 10points
    //if >=50 but < 90 then 4 points, >=25 but less than 50 then 2 points, less than 25 then no point
    //we need to find for each user who viewed the course, no of chapters he viewed for the course
    // and total no of chapters in the course. Then we can get idea of the user who viewed that specific
    //course generates how much score, then we can sum of score for all users viewed the course

    val viewsRdd = sc.textFile("C:/TrendyTech/week10_downloads/assignment_dataset/views")
    //read all files in directory  -> RDD["userid,chapterId,dateAndTime"]
    //remove entries duplicate like same userid and chapterId.

    val viewsRddNoDt = viewsRdd.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt)).distinct()
    //RDD[(userId,chapterId)] no duplicates
    //define a function to get courseId for a chapter

    def findCourseId(chapterId: Int): Int = {
      val mapRdd = rdd.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
      //RDD[(chapterId,courseId)]
      val courseId = mapRdd.filter(x => chapterId == x._1).first._2
      courseId
    }

    val viewsRddInput = viewsRddNoDt.map(x => (x._1.toInt, x._2.toInt, findCourseId(x._2.toInt)))
    //Rdd[(userId,chapterId,courseId)]
    //now we have to find rdd[(userId, totalChaptersRead, courseId)]

    val viewsRddInputManaged = viewsRddInput.map(x => (x._3, (x._2, x._1)))
    //      [(courseId, (userId, chapterId ))]


    //now let's calculated the total chapters read
    //change rdd to [(courseId, (userId, totalChaptersRead ))]
    viewsRddNoDt.reduceByKey((x, y) => x + y)

    //RDD[(userId,totalChaptersRead)]


  }
}
