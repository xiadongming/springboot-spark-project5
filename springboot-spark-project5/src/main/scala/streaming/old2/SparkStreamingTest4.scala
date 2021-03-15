package streaming.old2

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: xiadming
 * @Date: 2021/1/24 16:11
 * @描述:
 */
object SparkStreamingTest4 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingTest")
    /** 每3秒钟采集一次数据 */
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


    val wordToOne: DStream[(String, Int)] = lines.map(word => (word, 1))

    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))
    val value: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)
    value.print()

    /** 启动采集器 */
    ssc.start()

    /** 等待采集器关闭 */
    ssc.awaitTermination()

  }

}
