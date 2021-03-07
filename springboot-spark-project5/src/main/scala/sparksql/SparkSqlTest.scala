package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: xiadming
 * @Date: 2021/1/24 10:39
 * @描述:
 */
object SparkSqlTest {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\user.json")
    //  df.show()

    /** sql方式 */
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()

    /** dsl方式  */
    df.select("age", "name").show()

    /** 使用$的时候，需要引入  import spark.implicits._ */
    //df.select($"age").show
    //df.select('age).show

    /** rdd <=> dataFrame */
    val valueRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "abc", 32), (2, "def", 29)))
    val df3: DataFrame = valueRDD.toDF("id", "name", "age")
    //df3.show()
    val rdd: RDD[Row] = df3.rdd
    /** DataSet <=> dataFrame */
    val ds: Dataset[Person] = df3.as[Person]
    ds.show()
    val df4: DataFrame = ds.toDF()

    /** rdd => dataSet */
    val ds4: Dataset[Person] = valueRDD.map({
      case (id, name, age) => {
        Person(id, name, age)
      }
    }
    ).toDS()
    ds4.show()


    //关闭环境
    spark.stop()
  }
  case class Person(id: Int, name: String, age: Int) {

  }
}



