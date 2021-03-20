package streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @Date: 2021/3/15 23:23
 * @Desc: Streaming整合sparkSql
 */
object StreamingWithSparlSql {

  def main(args: Array[String]): Unit = {

    /**
     * 创建sparkStreaming的运行环境
     **/
    val conf = new SparkConf().setMaster("local[1]").setAppName("SpaekStreamingTest1")
    //每5秒一个批次
    val steamingContext: StreamingContext = new StreamingContext(conf, Seconds(3))
    steamingContext.checkpoint("output")

    /**
     * 接听端口数据
     **/
    val lines: ReceiverInputDStream[String] = steamingContext.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val sparkSql: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSql.implicits._

    words.foreachRDD(rdd => {
      val frameDF: DataFrame = rdd.toDF()
      frameDF.createOrReplaceTempView("table_order")

      sparkSql.sql("select * from table_order ")

    })





  }
}
