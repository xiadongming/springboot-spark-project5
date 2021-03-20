package streaming

/**
 * @Date: 2021/3/15 22:39
 * @Desc: transform 算子：DStream和RDD进行交互的算子
 */
object StreamingTransform {
  def main(args: Array[String]): Unit = {

    val tuple: (Int, Int) = (1, 2)
    println(tuple._1)
    println(tuple._2)


    val tuple2: (Int, Int,Int) = (1, 2,3)
    println(tuple2._3)

    val tuple1: (Int, (Int, Int, Int)) = (1, (3, 4, 5))

    println(tuple1._2._3)

  }
}
