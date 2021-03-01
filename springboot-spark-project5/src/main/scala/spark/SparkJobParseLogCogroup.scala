package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * cogroup对两个rdd中相同的key进行聚合
 * */
object SparkJobParseLogCogroup {

  def main(args: Array[String]): Unit = {
    /** spark的运行环境 */
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("logParse")
    val sparkContext = new SparkContext(sparkconf)
    val textFile = sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project3\\springboot-spark-project2\\logfile")
    val timeUrl: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")

      /** 时间段，url */
      (strings(0), strings(1))
    })
    /** 不同时间段，对应的url数量 */
    val timeUrlCount: RDD[(String, Int)] = timeUrl.groupByKey().map(timeUrl => {
      (timeUrl._1, timeUrl._2.size)
    })
    timeUrlCount.collect().foreach(println)
    println("================")
    val timePageNo: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
      (strings(0), strings(2))
    })

    val timePageCount: RDD[(String, Int)] = timePageNo.groupByKey().map(keyvalue => {
      (keyvalue._1, keyvalue._2.size)
    })
    timePageCount.collect().foreach(println)

    /** 进行cogroup */
    val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = timeUrlCount.cogroup(timePageCount)

    val result2: RDD[(String, List[Int], List[Int])] = result.map(context => {
      (context._1, context._2._1.toList, context._2._2.toList)
    })
    //result.collect().foreach(println)
    //result2.collect().foreach(println)
    sparkContext.stop()
  }
}
