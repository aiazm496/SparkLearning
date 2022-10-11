package com.akash.rdd

object RegexScala {
  def main(args: Array[String]): Unit = {

    val name: String = "akash 24,1995"

    val regex = """(\w+) (\d+),(\d+)""".r

    val result = name match {
      case regex => "matched"
      case _ => "not matched"
    }

    println(result)

  }
}
