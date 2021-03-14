package sparksql.project1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparksql.project1.mapper.TopNMapper
import sparksql.project1.model.DayVideoAccessTopn

import scala.collection.mutable.ListBuffer

/**
 * @Date: 2021/3/13 20:06
 * @Desc: 将目标数据装到数据库中
 */
object SparkSqlFormatJobClearnData2DB {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlFormatJobETL2").setMaster("local[1]")
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
    //sparkApiCount(sparkSql, dataDF)

    /**
     * 使用sparkSql的方式
     **/
    try {
      sparkSqlCount(sparkSql, dataDF)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def sparkSqlCount(sparkSql: SparkSession, dataDF: DataFrame): Unit = {
    dataDF.createOrReplaceTempView("logs_table")
    val sqlDF: DataFrame = sparkSql.sql("select day,cmsId,count(cmsId) as times from logs_table where cmsType = 'video' group by day,cmsId order by day asc , times desc ")

    sqlDF.show()

    /**
     * 将数据写入mysql
     * 依次循环每个partition
     * 每个partition插入数据库
     **/
    sqlDF.foreachPartition(partitionOfRecords => {
      val list2 = new ListBuffer[DayVideoAccessTopn]

      /**
       * 每个partition中的数据再进行循环
       **/
      partitionOfRecords.foreach(info => {
        val day: String = info.getAs[String]("day")
        val cmsId: Long = info.getAs[Long]("cmsId")
        val times: Long = info.getAs[Long]("times")
        list2.append(DayVideoAccessTopn(day, cmsId, times))

      })
      TopNMapper.insertDayVideoAccessTopn(list2)

    })
  }

  /**
   * spark api的方式统计 topN的数据
   * 暂时注掉
   */
  /*  def sparkApiCount(sparkSql: SparkSession, dataDF: DataFrame): Unit ={
      import sparkSql.implicits._
      val topNDF: Dataset[Row] = dataDF.filter($"day" === "20200812" && $"cmsType" === "video")
        .groupBy("day", "cmsId").agg(count("cmsId") as ("times")).orderBy($"times".desc)

      topNDF.show()
    }*/

}
