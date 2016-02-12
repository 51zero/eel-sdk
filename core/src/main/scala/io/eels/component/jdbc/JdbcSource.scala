package io.eels.component.jdbc

import java.sql.DriverManager

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Field, FrameSchema, Reader, Row, Source}

case class JdbcSource(url: String, query: String, props: JdbcSourceProps = JdbcSourceProps(100))
  extends Source with StrictLogging {

  override def readers: Seq[Reader] = {

    logger.debug(s"Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    val stmt = conn.createStatement()
    stmt.setFetchSize(props.fetchSize)

    logger.debug(s"Executing query [$query]...")
    val rs = stmt.executeQuery(query)

    val part = new Reader {

      override def close(): Unit = {
        logger.debug("Closing reader")
        rs.close()
        stmt.close()
        conn.close()
      }

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
          val fields = for ( k <- 1 to schema.columns.size ) yield Field(rs.getString(k))
          Row(schema.columns, fields.toList)
        }
      }
    }

    Seq(part)
  }

  lazy val schema: FrameSchema = {

    logger.debug(s"Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    val stmt = conn.createStatement()
    stmt.setFetchSize(1)
    stmt.setMaxRows(1)

    logger.debug(s"Executing query for schema [$query]...")
    val rs = stmt.executeQuery(query)

    val dialect = props.dialect.getOrElse(JdbcDialect(url))

    val columnCount = rs.getMetaData.getColumnCount
    logger.debug(s"Resultset column count is $columnCount")

    val cols = for ( k <- 1 to columnCount ) yield {
      Column(
        name = rs.getMetaData.getColumnLabel(k),
        `type` = dialect.fromJdbcType(rs.getMetaData.getColumnType(k)),
        nullable = rs.getMetaData.isNullable(k) == 1,
        precision = rs.getMetaData.getPrecision(k),
        scale = rs.getMetaData.getScale(k),
        None
      )
    }

    val schema = FrameSchema(cols.toList)

    logger.debug("Fetched schema: ")
    logger.debug(schema.print)

    schema
  }
}

case class JdbcSourceProps(fetchSize: Int, dialect: Option[JdbcDialect] = None)

