package sparksql.project1.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * @Date: 2021/3/13 15:01
 * @Desc:   SimpleDateFormat 线程不安全的
 *
 */
object DateUtils {
  val YYYYMMDDHHMM_TIME_FORMAT =  FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val tar_format =  FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  def parse(time: String) = {
    tar_format.format(new Date(getTime(time)))
  }

  def getTime(time: String) = {
    try{
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime()
    }catch {
      case e :Exception =>{
        println("time= "+time)
        e.printStackTrace()
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val str: String = parse("[12/Aug/2020:21:05:00 +0800]")
    println(str)
  }
}
