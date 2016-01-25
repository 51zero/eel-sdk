package com.sksamuel.hs

import java.sql.DriverManager

import scala.language.implicitConversions

trait Source {
  def loader: Iterator[Seq[String]]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

case class JdbcSource(url: String, query: String) extends Source {
  override def loader: Iterator[Seq[String]] = new Iterator[Seq[String]] {
    val conn = DriverManager.getConnection(url)
    val rs = conn.createStatement().executeQuery(query)
    override def hasNext: Boolean = rs.next()
    override def next(): Seq[String] = for ( k <- 1 to rs.getMetaData.getColumnCount ) yield rs.getString(k)
  }
}