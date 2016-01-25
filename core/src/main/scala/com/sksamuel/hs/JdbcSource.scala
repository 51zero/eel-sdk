package com.sksamuel.hs

import java.sql.DriverManager

case class JdbcSource(url: String, query: String) extends Source {
  override def loader: Iterator[Seq[String]] = new Iterator[Seq[String]] {
    lazy val conn = DriverManager.getConnection(url)
    lazy val rs = conn.createStatement().executeQuery(query)
    override def hasNext: Boolean = rs.next()
    override def next(): Seq[String] = for ( k <- 1 to rs.getMetaData.getColumnCount ) yield rs.getString(k)
  }
}
