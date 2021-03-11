package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Date: 2021/3/9 11:06
 * @Desc: dataFrame 相关操作
 *        dataFrame 存在列的名称
 */
object SparkSqlDataFrameTest2 {
  def main(args: Array[String]): Unit = {

    /** 创建sparkSql的运行环境 */
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlDataFrameTest2").setMaster("local[2]")
    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /** 引入sparkSql的隐式转换 */
    import sparkSql.implicits._
    /** 读取txt文件 ,需要*/
    val txtRDD: RDD[String] = sparkSql.sparkContext.textFile("D:\\11eclipaseworkspace2_idea\\springboot-spark-project5\\springboot-spark-project5\\data\\user.txt")

    val infoDF: DataFrame = txtRDD.map(_.split(",")).map(context => {
      info(context(0).toString(), context(1), context(2))
    }).toDF()
    //1-展示数据，默认20条数据
    infoDF.show()
    //查询某一列数据
    infoDF.filter(infoDF.col("age") > 30).show()


  }
  case class info(age:String,name:String,id:String)
}
