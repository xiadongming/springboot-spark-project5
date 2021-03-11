package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/11 21:20
 * @Desc:
 */
object SparkSqlHive {
  def main(args: Array[String]): Unit = {
    /** sparkSql的运行环境 */
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlDataFrame3")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    sparkSql.sql("show tables").show()


    sparkSql.stop()
  }

}
