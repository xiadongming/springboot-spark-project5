package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Date: 2021/3/15 20:51
 * @Desc:
     -- 开启端口  -- 这个就可以
     nc -l -p 9999
 */
object SpaekStreamingTest1 {

  def main(args: Array[String]): Unit = {
    /**
     * 创建sparkStreaming的运行环境
     **/
    val conf = new SparkConf().setMaster("local[1]").setAppName("SpaekStreamingTest1")
    //每5秒一个批次
    val steamingContext:StreamingContext = new StreamingContext(conf, Seconds(3))

    /**
     * 接听端口数据
     * */
    val lines: ReceiverInputDStream[String] = steamingContext.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val value: DStream[(String, Int)] = words.map(word => (word, 1))

    value.print()

    /** 启动采集器 */
    steamingContext.start()

    /** 等待采集器关闭 */
    steamingContext.awaitTermination()

  }
}
