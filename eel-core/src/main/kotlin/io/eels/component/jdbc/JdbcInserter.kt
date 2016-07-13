package io.eels.component.jdbc

import io.eels.Row
import io.eels.component.ResultSetIterator
import io.eels.schema.Schema
import io.eels.util.Logging
import io.eels.util.zipWithIndex
import java.sql.DriverManager

class JdbcInserter(val url: String,
                   val table: String,
                   val schema: Schema,
                   val autoCommit: Boolean,
                   val dialect: JdbcDialect) : Logging {

  val conn = DriverManager.getConnection(url)

  init {
    logger.debug("Connecting to jdbc $url...")
    conn.autoCommit = autoCommit
    logger.debug("Connected to $url")
  }

  fun insertBatch(batch: List<Row>): Unit {
    val stmt = conn.prepareStatement(dialect.insertQuery(schema, table))
    try {
      batch.forEach { row ->
        row.values.zipWithIndex().forEach {
          stmt.setObject(it.second + 1, it.first)
        }
        stmt.addBatch()
      }
      val result = stmt.executeBatch()
      logger.debug("Batch completed; ${result.size} rows inserted")
      if (!autoCommit) conn.commit()
    } catch (e: Exception) {
      logger.error("Batch failure", e)
      if (!autoCommit)
        conn.rollback()
      throw e
    } finally {
      stmt.close()
    }
  }

  fun ensureTableCreated(): Unit {
    logger.debug("Ensuring table [$table] is created")

    fun tableExists(): Boolean {
      logger.debug("Fetching list of tables to detect if $table exists")
      val tables = ResultSetIterator(conn.metaData.getTables(null, null, null, arrayOf("TABLE"))).asSequence().toList()
      val tableNames = tables.map { it.get(3).toLowerCase() }
      val exists = tableNames.contains(table.toLowerCase())
      logger.debug("${tables.size} tables found; $table exists == $exists")
      return exists
    }

    if (!tableExists()) {
      val sql = dialect.create(schema, table)
      logger.info("Creating table $table [$sql]")
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(sql)
        if (!autoCommit) conn.commit()
      } catch(e: Exception) {
        logger.error("Batch failure", e)
        if (!autoCommit)
          conn.rollback()
        throw e
      } finally {
        stmt.close()
      }
    }
  }
}