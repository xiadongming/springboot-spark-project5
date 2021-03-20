package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Date: 2021/3/20 10:30
 * @Desc: streaming对接kafka
 *       方式一：通过checkPoint的方式维护offset
 */
object StreamingKafkaOffset1 {


  def main(args: Array[String]): Unit = {

    val checkPoint = "D:\\idea_workspace\\springboot-spark-project5\\springboot-spark-project5\\data\\checkpoint"
    def functionCheckPint(): StreamingContext = {

      /**
       * 创建sparkStreaming的运行环境
       **/
      val conf = new SparkConf().setMaster("local[1]").setAppName("SpaekStreamingTest1")
      //每5秒一个批次
      val steamingContext: StreamingContext = new StreamingContext(conf, Seconds(3))
      steamingContext.checkpoint(checkPoint)
      //报错kafka的offset数据，保证不重复消费，不丢失数据
      steamingContext.checkpoint(checkPoint)
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "127.0.0.1:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "0001",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("weblogs")
      val streamDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        steamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      //接收到的数据
      streamDataDS.foreachRDD(rdd => {
        println("rdd中的记录数 " + rdd.count())
      })


      steamingContext
    }

    //从checkPoint路径下获取数据
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPoint, functionCheckPint)



    ssc.start()
    ssc.awaitTermination()

  }


}
