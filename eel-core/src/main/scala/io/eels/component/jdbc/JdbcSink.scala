package io.eels.component.jdbc

import java.sql.{Connection, DriverManager}

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Sink
import io.eels.component.jdbc.dialect.GenericJdbcDialect
import io.eels.schema.StructType

object JdbcSink extends Logging {

  private val config = ConfigFactory.load()
  private val warnIfMissingRewriteBatchedStatements = config.getBoolean("eel.jdbc.sink.warnIfMissingRewriteBatchedStatements")

  def apply(url: String, table: String): JdbcSink = {
    if (!url.contains("rewriteBatchedStatements")) {
      if (warnIfMissingRewriteBatchedStatements) {
        logger.warn("JDBC connection string does not contain the property 'rewriteBatchedStatements=true' which can be a major performance boost when writing data via JDBC. " +
          "Add this property to your connection string, or to remove this warning set eel.jdbc.warnIfMissingRewriteBatchedStatements=false")
      }
    }
    JdbcSink(() => DriverManager.getConnection(url), table)
  }
}

case class JdbcSink(connFn: () => Connection,
                    table: String,
                    createTable: Boolean = false,
                    batchSize: Int = 1000,
                    threads: Int = 4) extends Sink with Logging {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.jdbc.sink.bufferSize")
  private val autoCommit = config.getBoolean("eel.jdbc.sink.autoCommit")

  def withCreateTable(createTable: Boolean): JdbcSink = copy(createTable = createTable)
  def withBatchSize(batchSize: Int): JdbcSink = copy(batchSize = batchSize)
  def withThreads(threads: Int): JdbcSink = copy(threads = threads)

  override def open(schema: StructType) =
    new JdbcWriter(schema, connFn, table, createTable, new GenericJdbcDialect(), threads, batchSize, autoCommit, bufferSize)
}