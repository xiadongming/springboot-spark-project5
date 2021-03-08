package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Date: 2021/3/8 23:07
 * @Desc: dataFrame 转化 dataframe
 */
object SparkSqlDataFrame2DataSet {


  def main(args: Array[String]): Unit = {

    /** 创建sparkSql的运行环境 */
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlDataFrame2DataSet")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    /**
     *import sparkSql.implicits._
     * 中的 sparkSql 是 SparkSession.builder()的对象值，不是spark包中的
     * */
    import sparkSql.implicits._

    val valueRDD: RDD[String] = sparkSql.sparkContext.textFile("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.txt")
    val value: RDD[Array[String]] = valueRDD.map(context => {
      context.split(",")
    })
    value.foreach(println)

    val personDF: DataFrame = value.map(line => {
      Person2(line(0).toInt, line(1), line(2).toInt)
    }).toDF()

    personDF.show()

    personDF.filter(personDF.col("age") > 60).show()


    /**
     * 创建临时表，从临时表中获取数据
     * */
    personDF.createOrReplaceTempView("tempTable")
    sparkSql.sql("select * from tempTable").show()


    sparkSql.stop()

  }

  case class Person2(age:Int,name:String,id:Int)
}
