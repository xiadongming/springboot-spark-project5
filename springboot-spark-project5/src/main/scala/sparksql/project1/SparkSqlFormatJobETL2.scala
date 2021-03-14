package sparksql.project1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import sparksql.project1.utils.AccessConverUtils

/**
 * @Date: 2021/3/13 17:06
 * @Desc: 将 SparkSqlFormatJobETL 中的字段，分子url
 */
object SparkSqlFormatJobETL2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSqlFormatJobETL2").setMaster("local[1]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val wordRDD: RDD[String] = sparkSql.sparkContext.textFile("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\output\\part-00000")

    wordRDD.take(10).foreach(println)
    val valueRow: RDD[Row] = wordRDD.map(x => {
      AccessConverUtils.parse(x)
    })

    val loggerDF: DataFrame = sparkSql.createDataFrame(valueRow, AccessConverUtils.struct)
    loggerDF.show()
    // 按照"天"进行分区
    loggerDF.write.format("json").mode(SaveMode.Overwrite)
                                      .partitionBy("day")
                                      .save("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\clearn")

    sparkSql.stop()
  }
}
