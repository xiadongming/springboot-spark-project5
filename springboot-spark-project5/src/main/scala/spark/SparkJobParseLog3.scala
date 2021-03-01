package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 统计：不同时间段内网页编号的数量
 * */
object SparkJobParseLog3 {
  def main(args: Array[String]): Unit = {
    /**
     * 创建spark的运行环境
     * */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTest")
    val sparkContext = new SparkContext(conf)
    val textFile: RDD[String] = sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project3\\springboot-spark-project2\\logfile")

    val timePageNo: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
      (strings(0), strings(2))
    })
    val pageItera: RDD[(String, Iterable[String])] = timePageNo.groupByKey()
    /** 改变数据结构 */
    val result: RDD[(String, Int)] = pageItera.map(keyValue => {
      (keyValue._1, keyValue._2.size)
    })
    result.collect().foreach(println)
    /** 如下也是对的 */
    /*val result2: RDD[(String, String, Int)] = pageItera.map(keyValue => {
      (keyValue._1, keyValue._2.mkString, keyValue._2.size)
    })
    result2.collect().foreach(println)*/


    sparkContext.stop()
  }
}
