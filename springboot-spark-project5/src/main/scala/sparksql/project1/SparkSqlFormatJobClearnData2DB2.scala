package sparksql.project1

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparksql.project1.SparkSqlFormatJobClearnData2DB.sparkSqlCount

/**
 * @Date: 2021/3/14 10:08
 * @Desc: 需求二：统计top10,,
 *       按照ip统计top10,,
 */
object SparkSqlFormatJobClearnData2DB2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSqlFormatJobClearnData2DB2").setMaster("local[1]")
      //关闭分区字段的类型推导
      .set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /**
     * 会读取文件夹下的所有文件
     **/
    val dataDF: DataFrame = sparkSql.read.format("json").load("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\clearn")
    dataDF.printSchema()
    dataDF.show()

    /**
     * 使用sparkAPI的方式
     **/
    //sparkApiCount2(sparkSql, dataDF)

    /**
     * 使用sparkSql的方式
     **/
    sparkSqlCount2(sparkSql, dataDF)
  }

  def sparkSqlCount2(sparkSql: SparkSession, dataDF: DataFrame): Unit = {
    dataDF.createOrReplaceTempView("logs_table2")
    val sqlDF: DataFrame = sparkSql.sql("select day,city,ip ,count(cmsId) as times from logs_table2 where cmsType = 'video' and day = '20200812' group by day,city,ip order by day asc  ")


    sqlDF.show()
  }




   def sparkApiCount2(sparkSql: SparkSession, dataDF: DataFrame): Unit = {
     import sparkSql.implicits._
     val frameDF: DataFrame = dataDF.filter($"day" === "20200812" && $"cmsType" === "video"  ).groupBy("day", "city","ip", "cmsId").agg(count("cmsId").as("times"))

     dataDF.printSchema()
     frameDF.printSchema()


     val frame: DataFrame = frameDF.select(frameDF.col("day"),frameDF.col("day"),frameDF.col("ip"),
                                           frameDF.col("city"),frameDF.col("cmsId"),
                                           frameDF.col("times"),
                                           row_number().over(Window.partitionBy("city").
                                             orderBy(frameDF.col("times").desc)).as("times_rank"))//.filter("times_rank <= 10") //统计top10

     frame.show()

   }

}
