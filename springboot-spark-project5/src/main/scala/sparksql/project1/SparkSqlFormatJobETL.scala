package sparksql.project1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sparksql.project1.utils.DateUtils

/**
 * @Date: 2021/3/13 14:15
 * @Desc: 数据清洗，抽取出指定列的数据
 *        字段：ip地址，时间，url，
 */
object SparkSqlFormatJobETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlFormatJobETL").setMaster("local[1]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val wordRDD: RDD[String] = sparkSql.sparkContext.textFile("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\access.log")

    val value: RDD[String] = wordRDD.map(word => {
      val strings: Array[String] = word.split(" ")
      val ip: String = strings(0)
      val time = strings(3).replaceAll("\"", "") + " " + strings(4).replaceAll("\"", "")
      val strtime: String = time.replaceAll("\"", "")
      val url: String = strings(10)
      val triffic: String = strings(9) //流量
      DateUtils.parse(strtime) + "\t" + url.replaceAll("\"", "") + "\t" + triffic + "\t" + ip
    })

    /** 触发算子，才能进入debug中 */
    value.take(1000).foreach(println)
    try {
      value.saveAsTextFile("D:/idea_workspace/springboot-spark-project5/springboot-spark-project5/data/output")
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    sparkSql.stop()
  }
}
