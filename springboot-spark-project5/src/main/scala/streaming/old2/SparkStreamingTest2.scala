package streaming.old2

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @Author: xiadming
 * @Date: 2021/1/24 16:11
 * @描述:
 */
object SparkStreamingTest2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingTest")
    /** 每3秒钟采集一次数据 */
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    /** 获取数据 */
    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    messageDS.print()
    /** 启动采集器 */
    ssc.start()
    /** 等待采集器关闭 */
    ssc.awaitTermination()
  }
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (true) {
            val message: String = "采集的数据为：" + new Random().nextInt(10).toString
            /** 采集到数据之后，进行封装成 DStream */
            store(message)
            Thread.sleep(1000)
          }
        }
      }).start()
    }
    override def onStop(): Unit = {
      flag = false
    }
  }
}
