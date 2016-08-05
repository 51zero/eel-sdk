package io.eels.util

import java.sql.ResultSet

object ResultSetIterator {

  def apply(rs: ResultSet) = new Iterator[ResultSet] {
    override def hasNext(): Boolean = rs.next()

    override def next(): Array[String] = {
      val values = for (k <- 1 to rs.getMetaData.getColumnCount) yield {
        rs.getString(k)
      }
      values.toVector
    }
  }
}