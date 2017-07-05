package io.eels.component.jdbc

import java.sql.{Connection, DriverManager}

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Sink
import io.eels.component.jdbc.dialect.{GenericJdbcDialect, JdbcDialect}
import io.eels.schema.StructType
import com.sksamuel.exts.OptionImplicits._

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
                    batchesPerCommit: Int = 0, // 0 means commit at the end, otherwise how many batches before a commit
                    dialect: Option[JdbcDialect] = None,
                    threads: Int = 4) extends Sink with Logging {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.jdbc.sink.bufferSize")
  private val autoCommit = config.getBoolean("eel.jdbc.sink.autoCommit")

  def withCreateTable(createTable: Boolean): JdbcSink = copy(createTable = createTable)
  def withBatchSize(batchSize: Int): JdbcSink = copy(batchSize = batchSize)
  def withThreads(threads: Int): JdbcSink = copy(threads = threads)
  def withBatchesPerCommit(commitSize: Int): JdbcSink = copy(batchesPerCommit = batchesPerCommit)
  def withDialect(dialect: JdbcDialect): JdbcSink = copy(dialect = dialect.some)

  override def open(schema: StructType) =
    new JdbcWriter(schema, connFn, table, createTable, dialect.getOrElse(new GenericJdbcDialect), threads, batchSize, batchesPerCommit, autoCommit, bufferSize)
}