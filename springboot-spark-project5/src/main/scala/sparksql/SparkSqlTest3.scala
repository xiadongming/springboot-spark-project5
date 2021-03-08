package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Date: 2021/3/8 20:37
 * @Desc:
 */
object SparkSqlTest3 {

  def main(args: Array[String]): Unit = {

    /**   创建 sparksql 的运行环境*/
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlTest3").setMaster("local[*]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = sparkSql.read.json("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.json")
    df.show()

    sparkSql.stop()
  }
}
