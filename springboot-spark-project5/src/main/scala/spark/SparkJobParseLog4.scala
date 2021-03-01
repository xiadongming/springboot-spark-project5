package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
 * 统计：不同时间段内网页编号的值
 * */
object SparkJobParseLog4 {
  def main(args: Array[String]): Unit = {
    /** 创建spark的运行环境 */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTest")
    val sparkContext = new SparkContext(conf)
    val textFile: RDD[String] = sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project3\\springboot-spark-project2\\logfile")
    /**
     * 改变数据结构，将不同字段拆出来
     * */
    val timePageNo: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
      (strings(0), strings(2))
    })
    /**
     * 对字段进行聚合
     * */
    val aggByKey: RDD[(String, Iterable[String])] = timePageNo.groupByKey()
    /**
     * 对value进行操作
     * */
    val result: RDD[(String, List[String])] = aggByKey.map(keyValue => {
      (keyValue._1, keyValue._2.toList)
    })
    result.collect().foreach(println)
    sparkContext.stop()
  }
}
