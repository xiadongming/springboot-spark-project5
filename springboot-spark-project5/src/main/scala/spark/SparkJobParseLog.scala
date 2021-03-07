package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计日志
 * 统计每一个时间段内网站的访问量数量
 * 2020-01-01 https://www.baidu.com a
 * 时间    网站url  页面编号
 **/
object SparkJobParseLog {
  def main(args: Array[String]): Unit = {
    /** spark的运行环境 */
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("logParse")
    val sc = new SparkContext(sparkconf)
    val textFile = sc.textFile("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\logfile")
    val timeUrl: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")

      /** 时间段，url */
      (strings(0), strings(1))
    })
    /** 不同时间段，对应的url数量 */
    val value: RDD[(String, Int)] = timeUrl.groupByKey().map(timeUrl => {
      (timeUrl._1, timeUrl._2.size)
    })
   value.collect().foreach(println)
    sc.stop()
  }
}
