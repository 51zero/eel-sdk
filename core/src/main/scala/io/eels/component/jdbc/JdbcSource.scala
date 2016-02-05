package io.eels.component.jdbc

import java.sql.{DriverManager, Types}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Field, FrameSchema, Reader, Row, SchemaType, Source}

case class JdbcSource(url: String, query: String, props: JdbcSourceProps = JdbcSourceProps(100))
  extends Source with StrictLogging {

  override def reader: Reader = new Reader {

    logger.debug(s"Connecting to jdbc source $url...")
    private val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    private val stmt = conn.createStatement()
    logger.debug(s"Setting jdbc fetch size to ${props.fetchSize}")
    stmt.setFetchSize(props.fetchSize)

    logger.debug(s"Executing query [$query]...")
    private val rs = stmt.executeQuery(query)
    logger.debug(s"Query completed")

    private val columnCount = rs.getMetaData.getColumnCount
    logger.debug("Resultset column count is $columnCount")

    private val schema: FrameSchema = {
      val cols = for ( k <- 1 to columnCount ) yield {
        Column(
          name = rs.getMetaData.getColumnLabel(k),
          `type` = JdbcTypeToSchemaType(rs.getMetaData.getColumnType(k)),
          nullable = rs.getMetaData.isNullable(k) == 1,
          precision = rs.getMetaData.getPrecision(k),
          scale = rs.getMetaData.getScale(k),
          None
        )
      }
      FrameSchema(cols.toList)
    }
    logger.debug("Built schema from resultset: ")
    logger.debug(schema.print)

    override def iterator: Iterator[Row] = new Iterator[Row] {

      override def hasNext: Boolean = {
        val hasNext = rs.next()
        if (!hasNext) {
          logger.debug("Resultset is completed; closing stream")
          close()
        }
        hasNext
      }

      override def next: Row = {
        val fields = for ( k <- 1 to columnCount ) yield Field(rs.getString(k))
        Row(schema.columns, fields.toList)
      }
    }

    override def close(): Unit = {
      logger.debug("Closing reader")
      rs.close()
      stmt.close()
      conn.close()
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