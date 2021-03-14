package sparksql.project1.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @Date: 2021/3/13 21:11
 * @Desc:
 */
object MysqlUtils {

  def getConnection() = {
     DriverManager.getConnection("jdbc:mysql://localhost:3306/lucene", "root", "root")
  }

  def resease(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (null != pstmt) {
        pstmt.close()
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (null != connection) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {

   println(getConnection())

  }

}
