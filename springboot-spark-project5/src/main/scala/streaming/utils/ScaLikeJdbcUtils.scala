package streaming.utils
import scalikejdbc._
import scalikejdbc.config._
/**
 * @Date: 2021/3/20 11:38
 * @Desc: ScaLike 操作数据库
 */
object ScaLikeJdbcUtils {
  def main(args: Array[String]): Unit = {
    // scalike的配置文件加载
    DBs.setupAll()
    dbQuery()

    dbUpdate()

    dbInsert()


    DBs.closeAll()
  }

  def dbInsert() = {
    DB.autoCommit{
      implicit session =>{
        SQL("insert into offset_storage values(?,?,?,?) ")
          .bind(20000,"2","4","7").update().apply()
      }
    }
  }

   /**
    * 更新数据
    * */
  def dbUpdate() = {

    DB.autoCommit{
      implicit session =>{
        SQL("update offset_storage set offset = ? where topic=? and groupid=? and partitions=?")
          .bind(10000,"1","3","4").update().apply()
      }
    }
  }



  /**
   * 查询数据
   * */
  def dbQuery(): Unit = {

    DB.readOnly(implicit context =>{
     // SQL("select * from offset_storage").map(rs =>rs.long("offset")).list().apply().foreach(println)
      SQL("select * from offset_storage").map(rs =>offset(rs.string("topic"),rs.string("groupid"),rs.int("partitions"),rs.long("offset"))).list().apply().foreach(println)
    })
  }
case class offset(topic:String,groupid:String,partitions:Int,offset:Long)
}
