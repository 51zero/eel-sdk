package com.sksamuel.hs.source

import java.sql.DriverManager

import com.sksamuel.hs.Source
import com.sksamuel.hs.sink.{Field, Column, Row}

case class JdbcSource(url: String, query: String, props: JdbcSourceProps = JdbcSourceProps(100)) extends Source {

  override def loader: Iterator[Row] = new Iterator[Row] {

    lazy val conn = DriverManager.getConnection(url)

    lazy val rs = {
      val stmt = conn.createStatement()
      stmt.setFetchSize(props.fetchSize)
      stmt.executeQuery(query)
    }

    lazy val columns: Seq[Column] = {
      for ( k <- 1 to rs.getMetaData.getColumnCount ) yield {
        Column(rs.getMetaData.getColumnLabel(k))
      }
    }

    override def hasNext: Boolean = rs.next()

    override def next(): Row = {
      val fields = for ( k <- 1 to rs.getMetaData.getColumnCount ) yield Field(rs.getString(k))
      Row(columns, fields)
    }
  }
}

case class JdbcSourceProps(fetchSize: Int)