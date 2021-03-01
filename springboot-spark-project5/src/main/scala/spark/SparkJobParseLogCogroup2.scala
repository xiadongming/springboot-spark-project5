package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * cogroup对两个rdd中相同的key进行聚合
 * */
object SparkJobParseLogCogroup2 {

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
    val timeUrlName: RDD[(String, List[String])] = timeUrl.groupByKey().map(timeUrl => {
      (timeUrl._1, timeUrl._2.toList)
    })
    //timeUrlName.collect().foreach(println)
    println("================")
    val timePageNo: RDD[(String, String)] = textFile.map(total => {
      val strings: Array[String] = total.split(" ")
      (strings(0), strings(2))
    })

    val timePageName: RDD[(String, List[String])] = timePageNo.groupByKey().map(keyvalue => {
      (keyvalue._1, keyvalue._2.toList)
    })
    //timePageName.collect().foreach(println)

    /** 进行cogroup */
    val result: RDD[(String, (Iterable[List[String]], Iterable[List[String]]))] = timePageName.cogroup(timeUrlName)

    val result2: RDD[(String, List[String], List[String])] = result.map(context => {
      (context._1, context._2._1.head, context._2._2.head)
    })
    //result.collect().foreach(println)
    result2.collect().foreach(println)
    sparkContext.stop()
  }
}
