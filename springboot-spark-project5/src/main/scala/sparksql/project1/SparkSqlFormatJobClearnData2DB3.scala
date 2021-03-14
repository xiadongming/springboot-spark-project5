package sparksql.project1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/14 11:17
 * @Desc: 按照day,cmsId统计流量的topn
 */
object SparkSqlFormatJobClearnData2DB3 {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSqlFormatJobClearnData2DB3").setMaster("local[1]")
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
     * 使用sparkSql的方式
     **/
    sparkSqlCount3(sparkSql, dataDF)


  }

  def sparkSqlCount3(sparkSql: SparkSession, dataDF: DataFrame): Unit = {

    /**
     * day是空的，使用ip代替
     **/
    dataDF.createOrReplaceTempView("logger_table")
    val frameDF: DataFrame = sparkSql.sql("select ip,cmsId,count(triffic) from logger_table group by ip,cmsId")

    frameDF.show()
  }

}
