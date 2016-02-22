package io.eels.component.jdbc

import java.sql.DriverManager

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Reader, InternalRow, Source}

import scala.concurrent.duration._

case class JdbcSource(url: String, query: String, props: JdbcSourceProps = JdbcSourceProps(100))
  extends Source
    with StrictLogging
    with Using {

  override def readers: Seq[Reader] = {

    logger.info(s"Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    val stmt = conn.createStatement()
    stmt.setFetchSize(props.fetchSize)

    logger.debug(s"Executing query [$query]...")
    val start = System.currentTimeMillis()
    val rs = stmt.executeQuery(query)
    val duration = (System.currentTimeMillis() - start).millis
    logger.info(s" === query completed in $duration ===")

    val dialect = props.dialect.getOrElse(JdbcDialect(url))
    val schema = SchemaBuilder(rs, dialect)
    logger.debug("Fetched schema: ")
    logger.debug(schema.print)

    val columnCount = schema.columns.size

    val part = new Reader {

      override def close(): Unit = {
        logger.debug("Closing reader")
        rs.close()
        stmt.close()
        conn.close()
      }

      var created = false

      override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
        require(!created, "!Cannot create more than one iterator for a jdbc source!")
        created = true

        override def hasNext: Boolean = {
          val hasNext = rs.next()
          if (!hasNext) {
            logger.debug("Resultset is completed; closing stream")
            close()
          }
          hasNext
        }

        override def next: InternalRow = {
          for ( k <- 1 to columnCount ) yield {
            rs.getObject(k)
          }
        }
      }
    }

    Seq(part)
  }

  lazy val schema: FrameSchema = {

    logger.info(s"Connecting to jdbc source $url...")
    using(DriverManager.getConnection(url)) { conn =>
      logger.debug(s"Connected to $url")

      val stmt = conn.createStatement()

      val schemaQuery = s"SELECT * FROM ($query) tmp WHERE 1=0"
      logger.debug(s"Executing query for schema [$schemaQuery]...")
      val start = System.currentTimeMillis()
      val rs = stmt.executeQuery(query)
      val duration = (System.currentTimeMillis() - start).millis
      logger.info(s" === schema fetch completed in $duration ===")

      val dialect = props.dialect.getOrElse(JdbcDialect(url))
      val schema = SchemaBuilder(rs, dialect)
      rs.close()
      stmt.close()

      logger.debug("Fetched schema: ")
      logger.debug(schema.print)

      schema
    }
  }
}

case class JdbcSourceProps(fetchSize: Int, dialect: Option[JdbcDialect] = None)

