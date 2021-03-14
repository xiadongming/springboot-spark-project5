package sparksql.project1.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * @Date: 2021/3/13 17:14
 * @Desc:
 */
object AccessConverUtils {

  /** 定义输出的字段  */
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType), //video,artilc
      StructField("cmsId", LongType), //
      StructField("triffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType) // 分区字段
    )
  )

  /**
   * 输入参数
   **/
  def parse(log: String) = {
    try {
      val strings: Array[String] = log.split("\t")
      val url: String = strings(1)
      val triffic: Long = strings(2).toLong
      val ip: String = strings(3)
      val time: String = strings(0)
      val domain = "http://localhost:9123/admin/"
      val cms: String = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId: Array[String] = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city = ""
      val day: String = time.substring(0, 10).replaceAll("-", "")
      Row(url, cmsType, cmsId, triffic, ip,city, time, day)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Row(0)
      }
    }
  }
}
