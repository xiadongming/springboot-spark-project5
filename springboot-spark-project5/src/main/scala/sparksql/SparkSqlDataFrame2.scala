package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Date: 2021/3/10 22:15
 * @Desc:
 */
object SparkSqlDataFrame2 {

  def main(args: Array[String]): Unit = {
    /** 创建sparksql的运行环境 */
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlDataFrame2").setMaster("local[*]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSql.implicits._
    val valueRDD: RDD[String] = sparkSql.sparkContext.textFile("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\nginx.txt")
    val nginxDF =  valueRDD.map(context => {
      context.split("\\|")
    }).map(word => {
      nginxInfo(word(0), word(1), word(2), word(3))
    }).toDF()

    val nginxDF2 =  valueRDD.map(context => {
      context.split("\\|")
    }).map(word => {
      nginxInfo(word(0), word(1), word(2), word(3))
    }).toDF()


    //nginxDF.show(30)
   // nginxDF.select(nginxDF.col("name")).show()
    /** 查看10行记录 */
    val arr: Array[Row] = nginxDF.take(10)
    arr.foreach(println)

    val row: Row = nginxDF.first()
    println(row)

    //过滤字符
    nginxDF.filter("name ='' or name = 'null'").show()
    //排序
    nginxDF.sort("name").show()
    nginxDF.sort(nginxDF("name").desc).show()
    nginxDF.sort(nginxDF("name").asc).show()


    //join操作,默认内连接
    nginxDF2.join(nginxDF,nginxDF2.col("id") === nginxDF.col("id")).show()
    sparkSql.stop()
  }

  case class nginxInfo(id:String,name:String ,phoneNum:String,email:String)
}
