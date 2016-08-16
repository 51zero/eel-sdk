package io.eels.component.jdbc

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Sink
import io.eels.schema.Schema

case class JdbcSink(url: String,
                    table: String,
                    createTable: Boolean = false,
                    batchSize: Int = 1000,
                    threads: Int = 4) extends Sink with Logging {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.jdbc.sink.bufferSize")
  private val autoCommit = config.getBoolean("eel.jdbc.sink.autoCommit")
  private val warnIfMissingRewriteBatchedStatements = config.getBoolean("eel.jdbc.sink.warnIfMissingRewriteBatchedStatements")
  private val swallowExceptions = config.getBoolean("eel.jdbc.sink.swallowExceptions")

    if (!url.contains("rewriteBatchedStatements")) {
      if (warnIfMissingRewriteBatchedStatements) {
        logger.warn("JDBC connection string does not contain the property 'rewriteBatchedStatements=true' which can be a major performance boost when writing data via JDBC. " +
            "Add this property to your connection string, or to remove this warning set eel.jdbc.warnIfMissingRewriteBatchedStatements=false")
      }
  }

  override def writer(schema: Schema) =
      new JdbcWriter(schema, url, table, createTable, new GenericJdbcDialect(), threads, batchSize, autoCommit, bufferSize)
}