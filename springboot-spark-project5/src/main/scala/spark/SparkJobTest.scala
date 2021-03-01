package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取excel，乱码
 * */
object SparkJobTest {
  def main(args: Array[String]): Unit = {

    /** 创建spark的运行环境 */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTest")
    val sparkContext = new SparkContext(conf)
    val textFile: RDD[String] = sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project3\\springboot-spark-project2\\123.xlsx")
    val value: RDD[String] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
      strings(0)
    })
    value.collect().foreach(println)

    sparkContext.stop()
  }
}
