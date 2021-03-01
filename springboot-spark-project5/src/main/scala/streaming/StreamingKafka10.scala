package streaming

import com.google.common.eventbus.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * sparkStreaming整合kafka
 * */
object StreamingKafka10 {
  /* def main(args: Array[String]): Unit = {
   val spark  = SparkSession.builder().master("local[2]").appName("streaming").getOrCreate()
    val sc =spark.sparkContext;
    val ssc = new StreamingContext(sc, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("weblogs")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines: DStream[String] = stream.map(x => x.value())
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordCounts: DStream[(String, Long)] = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }*/
}
