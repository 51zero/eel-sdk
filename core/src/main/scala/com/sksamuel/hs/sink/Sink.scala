package com.sksamuel.hs.sink

import java.sql.DriverManager

import com.typesafe.scalalogging.slf4j.StrictLogging

trait Sink {
  def insert(row: Seq[String])
  def completed(): Unit
}

case class JdbcSink(url: String, table: String) extends Sink with StrictLogging {

  private lazy val conn = DriverManager.getConnection(url)

  private lazy val columns = {
    val rs = conn.getMetaData.getColumns(null, null, null, null)
    val columns = Iterator.continually(rs.next)
      .takeWhile(_ == true)
      .filter(_ => rs.getString(3).toLowerCase == table.toLowerCase)
      .map(_ => rs.getString(4)).toList
    logger.debug(s"Fetched columns for $table: $columns")
    rs.close()
    columns
  }

  override def completed(): Unit = conn.close()

  override def insert(row: Seq[String]): Unit = {
    val stmt = s"INSERT INTO $table (${columns.mkString(",")}) VALUES (${row.mkString("'", "','", "'")})"
    conn.createStatement().executeUpdate(stmt)
  }
}
