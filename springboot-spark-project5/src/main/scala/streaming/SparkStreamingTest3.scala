package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: xiadming
 * @Date: 2021/1/24 16:11
 * @描述:
 */
object SparkStreamingTest3 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingTest")
    /** 每3秒钟采集一次数据 */
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("output")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val value: DStream[(String, Int)] = words.map(word => (word, 1))

    /**
     * seq:表示相同的key的value值
     * opt：表示在缓存区中，相同的key的value值
     * */
    val value1: DStream[(String, Int)] = value.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
      val newCount = buff.getOrElse(0) + seq.sum
      Option(newCount)
    })

    value1.print()
    /** 启动采集器 */
    ssc.start()

    /** 等待采集器关闭 */
    ssc.awaitTermination()

  }

}
