package com.sksamuel.eel

import java.sql.ResultSet

object ResultsetIterator {
  def apply(rs: ResultSet): Iterator[Array[String]] = new Iterator[Array[String]] {
    override def hasNext: Boolean = rs.next()
    override def next(): Array[String] = {
      for ( k <- 1 to rs.getMetaData.getColumnCount ) yield rs.getString(k)
    }.toArray
  }
}
