package io.eels.component.jdbc

import java.sql.{DriverManager, ResultSetMetaData}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, FrameSchema, Reader, Row, Source}

case class JdbcSource(url: String, query: String, props: JdbcSourceProps = JdbcSourceProps(100))
  extends Source
    with StrictLogging {

  override def readers: Seq[Reader] = {

    logger.info(s"Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    val stmt = conn.createStatement()
    stmt.setFetchSize(props.fetchSize)

    logger.debug(s"Executing query [$query]...")
    val rs = stmt.executeQuery(query)
    logger.debug("Query completed")

    val _schema = schema
    logger.debug("Schema=" + _schema)

    val part = new Reader {

      override def close(): Unit = {
        logger.debug("Closing reader")
        rs.close()
        stmt.close()
        conn.close()
      }

      var created = false

      override def iterator: Iterator[Row] = new Iterator[Row] {
        require(!created, "Cannot create more than one iterator for a jdbc source")
        created = true

        override def hasNext: Boolean = {
          val hasNext = rs.next()
          if (!hasNext) {
            logger.debug("Resultset is completed; closing stream")
            close()
          }
          hasNext
        }

        override def next: Row = {
          for ( k <- 1 to _schema.columns.size ) yield rs.getString(k)
        }
      }
    }

    Seq(part)
  }

  lazy val schema: FrameSchema = {

    logger.info(s"Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    val stmt = conn.createStatement()

    val schemaQuery = s"SELECT * FROM ($query) tmp WHERE 1=0"
    logger.debug(s"Executing query for schema [$schemaQuery]...")
    val rs = stmt.executeQuery(query)
    logger.debug("Query completed")

    val dialect = props.dialect.getOrElse(JdbcDialect(url))

    val md: ResultSetMetaData = rs.getMetaData
    val columnCount = md.getColumnCount
    logger.debug(s"Resultset column count is $columnCount")

    val cols = for ( k <- 1 to columnCount ) yield {
      Column(
        name = md.getColumnLabel(k),
        `type` = dialect.fromJdbcType(md.getColumnType(k)),
        nullable = md.isNullable(k) == 1,
        precision = md.getPrecision(k),
        scale = md.getScale(k),
        signed = md.isSigned(k),
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

