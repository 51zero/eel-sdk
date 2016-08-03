package io.eels.util

import java.sql.ResultSet

class ResultSetIterator(val rs: ResultSet) : Iterator<Array<String?>> {

  override fun hasNext(): Boolean = rs.next()

  override fun next(): Array<String?> {
    val list = mutableListOf<String?>()
    for (k in 1..rs.metaData.columnCount) {
      list.add(rs.getString(k))
    }
    return list.toTypedArray()
  }
}