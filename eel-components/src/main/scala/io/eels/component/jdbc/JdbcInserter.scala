package io.eels.component.jdbc

import java.sql.Connection

import com.sksamuel.exts.Logging
import com.sksamuel.exts.jdbc.ResultSetIterator
import io.eels.Row
import io.eels.schema.Schema

class JdbcInserter(val connFn: () => Connection,
                   val table: String,
                   val schema: Schema,
                   val autoCommit: Boolean,
                   val dialect: JdbcDialect) extends Logging {

  logger.debug("Connecting to JDBC to insert.. ..")
  val conn = connFn()
  conn.setAutoCommit(autoCommit)
  logger.debug(s"Connected successfully; autoCommit=$autoCommit")

  def insertBatch(batch: Seq[Row]): Unit = {
    val stmt = conn.prepareStatement(dialect.insertQuery(schema, table))
    try {
      batch.foreach { row =>
        row.values.zipWithIndex.foreach { case (value, k) =>
          stmt.setObject(k + 1, value)
        }
        stmt.addBatch()
      }
      val result = stmt.executeBatch()
      if (!autoCommit) conn.commit()
    } catch {
      case t: Throwable =>
        logger.error("Batch failure", t)
        if (!autoCommit)
          conn.rollback()
        throw t
    } finally {
      stmt.close()
    }
  }

  def ensureTableCreated(): Unit = {
    logger.info(s"Ensuring table [$table] is created")

    def tableExists(): Boolean = {
      logger.debug(s"Fetching list of tables to detect if $table exists")
      val tables = ResultSetIterator.strings(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).toList
      val tableNames = tables.map(x => x(3).toLowerCase)
      val exists = tableNames.contains(table.toLowerCase())
      logger.debug(s"${tables.size} tables found; $table exists == $exists")
      exists
    }

    if (!tableExists()) {
      val sql = dialect.create(schema, table)
      logger.info(s"Creating table $table [$sql]")
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(sql)
        if (!autoCommit) conn.commit()
      } catch {
        case t: Throwable =>
          logger.error("Batch failure", t)
          if (!autoCommit)
            conn.rollback()
          throw t
      } finally {
        stmt.close()
      }
    }
  }
}