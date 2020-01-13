package project.dao

import com.leahy.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.domain.CourseClickCount

import scala.collection.mutable.ListBuffer

/**
 * 实战课程点击数据数据访问层
 */
object CourseClickCountDAO {

  val tableName = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  /**
   * 保存数据到HBase
   * @param list CourseClickCount集合
   */
  def save(list : ListBuffer[CourseClickCount]) = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }

  /**
   * 根据RowKey查询值
   * @param day_course 定义的HBase RowKey
   */
  def count(day_course:String) = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8", 8))
    list.append(CourseClickCount("20171111_9", 10))

    save(list)
  }
}
