package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Date: 2021/3/15 23:13
 * @Desc:
 */
object StreamingForeachRDD {

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

    /**
     * 第一种：推荐
     **/
    words.foreachRDD(rdd => {
      /**
       * 按照分区遍历数据
       **/
      rdd.foreachPartition(partiton => {
        // 插入数据库

      })
    })

    /**
     * 第二种 :不推荐
     **/
    words.foreachRDD(rdd => {
      /**
       * 遍历rdd中的记录
       **/
      rdd.foreach(reconds => {
      })
    })
  }
}
