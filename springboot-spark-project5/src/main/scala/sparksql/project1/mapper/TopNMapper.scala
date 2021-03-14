package sparksql.project1.mapper

import java.sql.{Connection, PreparedStatement}

import sparksql.project1.model.DayVideoAccessTopn
import sparksql.project1.utils.MysqlUtils

import scala.collection.mutable.ListBuffer

/**
 * @Date: 2021/3/13 22:52
 * @Desc:
 */
object TopNMapper {

  /**
   * 批量插入数据库
   **/
  def insertDayVideoAccessTopn(list: ListBuffer[DayVideoAccessTopn]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MysqlUtils.getConnection()

      /** 手动提交 */
      connection.setAutoCommit(false)

      val insertSql = "insert into day_video_access_topn(day,cms_id,times) values(?,?,?)"
      pstmt = connection.prepareStatement(insertSql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setLong(2, item.cmsId)
        pstmt.setLong(3, item.times)
        pstmt.addBatch()
      }


      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      MysqlUtils.resease(connection, pstmt)
    }


  }
}
