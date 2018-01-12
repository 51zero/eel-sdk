package io.eels.component.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection

import scala.collection.mutable.ListBuffer

case class Attribute(name: String, value: String)

case class ColumnFamily(name: String, attributes: Seq[Attribute])

case class HbaseStatistics(columnFamilies: Seq[ColumnFamily])


object HbaseStatistics {
  private val ATTRIBUTE_PATTERN = "((\\w*)\\s?(=>)\\s?'(\\w*))'".r

  def apply(namespace: String, tableName: String)(implicit connection: Connection): HbaseStatistics = {
    val table = connection.getTable(TableName.valueOf(namespace, tableName))
    val cfs = new ListBuffer[ColumnFamily]
    ATTRIBUTE_PATTERN.findAllMatchIn(table.getTableDescriptor.toString).foreach { m =>
      val (name, value) = (m.group(2), m.group(4))
      if (name == "NAME") cfs.append(ColumnFamily(value, Seq()))
      else cfs(cfs.length - 1) = ColumnFamily(cfs.last.name, cfs.last.attributes ++ Seq(Attribute(name, value)))
    }
    HbaseStatistics(cfs.toList)
  }

}
