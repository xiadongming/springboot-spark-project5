package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计：不同时间段 对应 的url的值
 * */
object SparkJobParseLog2 {

  def main(args: Array[String]): Unit = {

    /** 创建spark的运行环境 */
    val conf: SparkConf = new SparkConf().setAppName("SparkJobParseLog2").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val textFile = sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project3\\springboot-spark-project2\\logfile")
    /** 第一步：先获取对应的key-value键值对 */
    val timeUrl: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
      (strings(0), strings(1))
    })
    /** 第二部：进行聚合 */
    val valueItera: RDD[(String, Iterable[String])] = timeUrl.groupByKey()
    /** 第三步：获取key和value */
    val result: RDD[(String, List[String])] = valueItera.map(keyValue => {
      (keyValue._1, keyValue._2.toList)
    })
    result.collect().foreach(println)
    sparkContext.stop()
  }
}
