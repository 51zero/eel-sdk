package io.eels.component.jdbc

import com.typesafe.config.ConfigFactory
import io.eels.Sink
import io.eels.schema.Schema
import io.eels.util.Logging

data class JdbcSink(val url: String, val table: String,
                    val createTable: Boolean = false,
                    val batchSize: Int = 1000,
                    val threads: Int = 4) : Sink, Logging {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.jdbc.sink.bufferSize")
  private val autoCommit = config.getBoolean("eel.jdbc.sink.autoCommit")
  private val warnIfMissingRewriteBatchedStatements = config.getBoolean("eel.jdbc.sink.warnIfMissingRewriteBatchedStatements")
  private val swallowExceptions = config.getBoolean("eel.jdbc.sink.swallowExceptions")

  init {
    if (!url.contains("rewriteBatchedStatements")) {
      if (warnIfMissingRewriteBatchedStatements) {
        logger.warn("JDBC connection string does not contain the property 'rewriteBatchedStatements=true' which can be a major performance boost when writing data via JDBC. " +
            "Add this property to your connection string, or to remove this warning set eel.jdbc.warnIfMissingRewriteBatchedStatements=false")
      }
    }
  }

  override fun writer(schema: Schema) =
      JdbcWriter(schema, url, table, createTable, GenericJdbcDialect(), threads, batchSize, autoCommit, bufferSize, swallowExceptions)
}