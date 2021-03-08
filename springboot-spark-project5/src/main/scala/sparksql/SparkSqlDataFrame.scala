package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/8 20:37
 * @Desc: dataFrame 相关操作
 */
object SparkSqlDataFrame {

  def main(args: Array[String]): Unit = {

    /** 创建 sparksql 的运行环境 */
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlTest3").setMaster("local[*]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = sparkSql.read.json("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.json")

    //1-查看约束
    df.printSchema()
    //2-查看内容，默认20条 ，通过df.show(100)自定义
    df.show()

    //3-查询某一列
    df.select("name").show()

    //3-查询某一列，计算某一列
    df.select(df.col("name"), df.col("age") + 100).show()

    //4- 起别名
    df.select(df.col("name").as("myName"), (df.col("age") + 200).as("myAge")).show()

    //5-查询年龄大于19的数据
    df.select(df.col("age") > 20).show()
    //5- 过滤年龄大于20的数据
    df.filter(df.col("age") > 20).show()

    //6-进行分组
    df.groupBy("age").count().show()
    df.groupBy(df.col("age")).count().show()
    df.groupBy("name").avg("age").show()


    sparkSql.stop()
  }
}
