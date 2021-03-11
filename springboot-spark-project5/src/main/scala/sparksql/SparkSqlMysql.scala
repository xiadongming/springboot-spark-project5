package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/11 21:26
 * @Desc: sparkSql操作mysql数据库
 */
object SparkSqlMysql {

  def main(args: Array[String]): Unit = {
    /** sparkSql的运行环境 */
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlDataFrame3")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    /** 加载mysql表中的数据 */
    val mysqlTableDF: DataFrame = sparkSql.read.format("jdbc")
                                          .option("url", "jdbc:mysql://localhost:3306/lucene")
                                          .option("dbtable", "house")
                                          .option("user", "root")
                                          .option("password", "root")
                                          .option("driver", "com.mysql.jdbc.Driver").load()
    sparkSql.sql("show tables").show()
    mysqlTableDF.printSchema()
    mysqlTableDF.show()


  }

}
