package spark

/**
 * @Date: 2021/3/2 18:06
 * @Desc: 拉链
 */
object SparkJobZipLog {

  def main(args: Array[String]): Unit = {
    val intList: List[Int] = List(1, 2, 3, 4)
    val stringList: List[String] = List("a", "b", "c", "d", "e")
    val tuples: List[(Int, String)] = intList.zip(stringList)

    tuples.foreach(println)
    println("===========")
    val tuples1: List[(String, Int)] = stringList.zip(intList)
    tuples1.foreach(println)

    println("============")

  }
}
