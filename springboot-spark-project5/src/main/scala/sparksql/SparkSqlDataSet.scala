package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import sparksql.SparkSqlDataFrame2.nginxInfo

/**
 * @Date: 2021/3/10 23:08
 * @Desc:
 */
object SparkSqlDataSet {
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

    //df转ds
    val valueDS: Dataset[nginxInfo] = nginxDF.as[nginxInfo]

    //rdd转ds
    val nginxDS =  valueRDD.map(context => {
      context.split("\\|")
    }).map(word => {
      nginxInfo(word(0), word(1), word(2), word(3))
    }).toDS()

    valueDS.show()
    nginxDS.show()



    sparkSql.stop()

  }
  case class nginxInfo(id:String,name:String ,phoneNum:String,email:String)
}
