package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/11 21:02
 * @Desc:
 */
object SparkSqlDataFrame4 {

  def main(args: Array[String]): Unit = {
    /** sparkSql的运行环境 */
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlDataFrame3")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSql.implicits._
    val frameDF: DataFrame = sparkSql.read.json("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.json")
    frameDF.select("name").show()

    /** 报错 */
   // val frame2DF: DataFrame = sparkSql.read.load("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.txt")
   // frame2DF.select("name").show()

    /** 需要指定格式 */
    val frame3DF: DataFrame = sparkSql.read.format("json").load("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.json")
    frame3DF.select("name").show()

    sparkSql.stop()
  }
}
