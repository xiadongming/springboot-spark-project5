package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/11 20:53
 * @Desc: 读取并保存到文件中
 */
object SparkSqlDataFrame3 {

  def main(args: Array[String]): Unit = {
    /** sparkSql的运行环境 */
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlDataFrame3")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSql.implicits._
    val frameDF: DataFrame = sparkSql.read.json("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.json")

    frameDF.select("name").show()

    /** 保存到文件 */
    frameDF.select("name").write.format("json").save("file:/D:/idea_workspace/springboot-spark-project5/springboot-spark-project5/data/user2.json")

    sparkSql.stop()

  }
}
