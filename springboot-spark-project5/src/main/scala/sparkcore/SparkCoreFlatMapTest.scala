package sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Date: 2021/3/9 14:37
 * @Desc: flatMap 操作
 */
object SparkCoreFlatMapTest {

  def main(args: Array[String]): Unit = {
    /** 创建sparkSql的运行环境 */
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlDataFrameTest2").setMaster("local[2]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val valueRDD: RDD[String] = sparkSql.sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project5\\springboot-spark-project5\\data\\user.txt")
    val flatMapRDD: RDD[String] = valueRDD.flatMap(_.split(","))
    /**
     * 通过flatMap将数据全部拆开，
     * 最好的使用场景是： 单词累加
     * */
    flatMapRDD.foreach(println)
    val valueMap: RDD[(String, Int)] = flatMapRDD.map(context => {
      (context, 1)
    })
    valueMap.foreach(println)

  }
}
