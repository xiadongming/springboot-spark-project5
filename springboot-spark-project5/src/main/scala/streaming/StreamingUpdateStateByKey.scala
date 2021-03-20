package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Date: 2021/3/15 22:55
 * @Desc: updateStateByKey 算子
 *        带状态的算子
 */
object StreamingUpdateStateByKey {

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

    val value: DStream[(String, Int)] = words.map(word => (word, 1))
    // value updateStateByKey[Int] (funUpdateStateByKey _ )

    value.saveAsTextFiles("ancv")


  }

  def funUpdateStateByKey(newValue: Seq[Int], runningCount: Option[Int]) {
    val curren = newValue.sum
    val i: Int = runningCount.getOrElse(0)
    Some(curren + i)
  }
}
