package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
 * @Date: 2021/3/20 11:13
 * @Desc: streaming对接kafka，维护offset
 *        方式二：将offset维护到mysql中
 */
object StreamingKafkaOffset2 {

  def main(args: Array[String]): Unit = {
    /**
     * 创建sparkStreaming的运行环境
     **/
    val conf = new SparkConf().setMaster("local[1]").setAppName("SpaekStreamingTest1")
    //每5秒一个批次
    val steamingContext: StreamingContext = new StreamingContext(conf, Seconds(3))
    // scalike的配置文件加载
    DBs.setupAll()

    val groupId = "testGroup"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
     * 从数据库获取offset
     **/
    val topics = Array("kafka-test-topic")
    val fromOffsets: collection.Map[TopicPartition, Long]  = DB.readOnly {
      implicit session => {
        SQL("select * from offset_storage where groupid= ? and topic=? ").bind(groupId, topics.head).map(rs => {
          (new TopicPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
        }).list().apply()
      }
    }.toMap

    val streamDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      steamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams,fromOffsets)
    )
    //业务处理，消费offset

    //接收到的数据
    streamDataDS.foreachRDD(rdd => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition {
        iter => {
          val o: OffsetRange = offsetRanges(TaskContext.get().partitionId())
          println(o.topic + "_" + o.partition + "_" + o.fromOffset + "_" + o.untilOffset)

        }

          /**
           * 保存offset到数据库中
           **/
          offsetRanges.map(x => {
            DB.autoCommit { implicit session => {
              SQL("INSERT INTO offset_storage (`topic`, `groupid`, `partitions`, `offset`) VALUES (?, ?, ?, ?) on DUPLICATE  key update offset = ?")
                .bind(x.topic, groupId, x.partition, x.untilOffset, x.untilOffset).update().apply()
            }
            }
          })
      }


    })

  }

}
