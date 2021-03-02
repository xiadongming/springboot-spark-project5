package streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Date: 2021/3/2 16:59
 * @Desc: streaming坚挺端口，获取端口的数据,
 *       未实现
 */
object SaprkStreamingListenPort {
  def main(args: Array[String]): Unit = {
    /** 获取 streaming的运行环境 */
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SaprkStreamingListenPort")
    val sc: SparkContext = new SparkContext(sparkConf)
    val streamingContext: StreamingContext = new StreamingContext(sc, Seconds(5))
    val portContext: ReceiverInputDStream[String] = streamingContext.socketTextStream("127.0.0.1", 9123, StorageLevel.MEMORY_AND_DISK_SER)

    println(portContext)

  }

}
