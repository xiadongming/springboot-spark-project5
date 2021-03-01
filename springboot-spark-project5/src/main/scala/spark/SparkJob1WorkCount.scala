package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 最简单的wordCount案例
 **/
object SparkJob1WorkCount {
  def main(args: Array[String]): Unit = {

    /** 创建spark的运行环境 */
    val conf = new SparkConf().setAppName("sparkJob1").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val textFile = sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project3\\springboot-spark-project2\\logfile")
    val urlContext: RDD[(String, Int)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
     // println(strings.mkString)
      (strings(1), 1)
    })
    // urlContext.collect().foreach(println)

    val wordCount: RDD[(String, Int)] = urlContext.reduceByKey(_ + _)
    wordCount.collect().foreach(println)
   // val value: RDD[(String, Iterable[Int])] = urlContext.groupByKey()
    sparkContext.stop()
  }
}
