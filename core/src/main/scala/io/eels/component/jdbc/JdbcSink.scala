package io.eels.component.jdbc

import java.sql.{Connection, DriverManager}

import com.sksamuel.scalax.jdbc.ResultSetIterator
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

case class JdbcSink(url: String, table: String, props: JdbcSinkProps = JdbcSinkProps())
  extends Sink
    with StrictLogging {

  private def tableExists(conn: Connection): Boolean = {
    logger.debug("Fetching tables to detect if table exists")
    val tables = ResultSetIterator(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).toList
    logger.debug(s"${tables.size} tables found")
    tables.map(_.apply(3).toLowerCase) contains table.toLowerCase
  }

  override def writer: Writer = new Writer {

    val dialect = props.dialectFn(url)
    logger.debug(s"Writer will use dialect=$dialect")

    logger.debug(s"Connecting to jdbc sink $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    var created = false

    def createTable(row: Row): Unit = {
      if (!created && props.createTable && !tableExists(conn)) {
        logger.info(s"Creating sink table $table")

        val sql = dialect.create(FrameSchema(row.columns), table)
        logger.debug(s"Executing [$sql]")

        val stmt = conn.createStatement()
        try {
          stmt.executeUpdate(sql)
        } finally {
          stmt.close()
        }
      }
      created = true
    }

    def doBatch(): Unit = {
      logger.info(s"Inserting batch [${buffer.size} rows]")
      val stmt = conn.createStatement()
      buffer.foreach(stmt.addBatch)
      try {
        stmt.executeBatch()
        logger.info("Batch complete")
      } catch {
        case e: Exception =>
          logger.error("Batch failure", e)
          throw e
      } finally {
        stmt.close()
        buffer.clear()
      }
    }

    override def close(): Unit = {
      if (buffer.nonEmpty)
        doBatch()
      conn.close()
    }

    val buffer = new ArrayBuffer[String](props.batchSize)

    override def write(row: Row): Unit = {
      createTable(row)

      val sql = dialect.insert(row, table)
      logger.debug(s"Buffering [$sql]")
      buffer.append(sql)

      if (buffer.size == props.batchSize) {
        doBatch()
      }
    }
  }
}

case class JdbcSinkProps(createTable: Boolean = false,
                         batchSize: Int = 100,
                         dialectFn: String => JdbcDialect = url => JdbcDialect(url))

