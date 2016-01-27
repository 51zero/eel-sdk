package com.fiftyonezero.eel.source

import java.sql.{DriverManager, Types}

import com.fiftyonezero.eel.{Reader, Source, FrameSchema, Row, SchemaType, Column, Field}
import com.sksamuel.scalax.jdbc.ResultSetIterator
import com.typesafe.scalalogging.slf4j.StrictLogging

case class JdbcSource(url: String, query: String, props: JdbcSourceProps = JdbcSourceProps(100))
  extends Source with StrictLogging {

  override def reader: Reader = new Reader {

    logger.debug(s"Connecting to jdbc source [$url]")
    private val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    val tables = ResultSetIterator(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).toList
    println(tables.toList.map(_.toList))

    private val stmt = conn.createStatement()
    stmt.setFetchSize(props.fetchSize)
    private val rs = stmt.executeQuery(query)

    override def iterator: Iterator[Row] = new Iterator[Row] {

      override def hasNext: Boolean = {
        val hasNext = rs.next()
        if (!hasNext)
          close()
        hasNext
      }

      override def next: Row = {
        val fields = for ( k <- 1 to rs.getMetaData.getColumnCount ) yield Field(rs.getString(k))
        Row(schema.columns, fields)
      }
    }

    override def close(): Unit = {
      rs.close()
      stmt.close()
      conn.close()
    }

    private def schema: FrameSchema = {
      val cols = for ( k <- 1 to rs.getMetaData.getColumnCount ) yield {
        Column(
          name = rs.getMetaData.getColumnLabel(k),
          `type` = JdbcTypeToSchemaType(rs.getMetaData.getColumnType(k)),
          nullable = rs.getMetaData.isNullable(k) == 1
        )
      }
      FrameSchema(cols)
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