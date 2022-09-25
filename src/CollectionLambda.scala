object CollectionLambda {

  def main(args: Array[String]): Unit = {


    val array = Array(1, 2, 3, 4, 5)

    val array2 = array.map(x => "a")

    println(array2.mkString(","))

    val array3 = array.flatMap(x=> Array(x))
    println(array3.mkString(","))

    val array4 = array.sortBy(x=>x)
    println(array4.mkString(","))

  }
}