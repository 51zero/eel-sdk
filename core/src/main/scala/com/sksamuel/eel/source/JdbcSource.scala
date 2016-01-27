package com.sksamuel.eel.source

import java.sql.{DriverManager, Types}

import com.sksamuel.eel.{Column, Field, Row, SchemaType, Source}

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
        Column(
          name = rs.getMetaData.getColumnLabel(k),
          `type` = JdbcTypeToSchemaType(rs.getMetaData.getColumnType(k)),
          nullable = rs.getMetaData.isNullable(k) == 1
        )
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

object JdbcTypeToSchemaType {
  def apply(int: Int): SchemaType = {
    int match {
      case Types.BIGINT => SchemaType.BigInt
      case Types.SMALLINT | Types.TINYINT | Types.INTEGER => SchemaType.Int
      case Types.BOOLEAN => SchemaType.Boolean
      case Types.DOUBLE => SchemaType.Double
      case Types.FLOAT => SchemaType.Float
      case Types.DECIMAL => SchemaType.Decimal
      case Types.DATE => SchemaType.Date
      case _ => SchemaType.String
    }
  }
}