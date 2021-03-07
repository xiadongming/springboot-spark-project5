package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: xiadming
 * @Date: 2021/1/24 10:39
 * @描述:
 */
object SparkSqlTest2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("data/user.json")
    //  df.show()

    /** sql方式 */
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()

    /** 自定义函数 prefixName */
    spark.udf.register("prefixName",(name:String) =>{
      "Name:"+name
    })
    spark.sql("select age,prefixName(name) from user ").show

    //关闭环境
    spark.stop()
  }
}



