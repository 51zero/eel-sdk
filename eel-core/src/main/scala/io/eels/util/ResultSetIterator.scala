package io.eels.util

import java.sql.ResultSet

object ResultSetIterator {

  def apply(rs: ResultSet) = new Iterator[ResultSet] {
    override def hasNext(): Boolean = rs.next()
    override def next(): ResultSet = rs
  }
}