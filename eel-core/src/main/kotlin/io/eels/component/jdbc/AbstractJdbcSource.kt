package io.eels.component.jdbc

import io.eels.util.Logging
import io.eels.schema.Schema
import io.eels.Source
import io.eels.util.Option
import io.eels.util.getOrElse
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

abstract class AbstractJdbcSource(val url: String,
                                  val providedSchema: Option<Schema>,
                                  val providedDialect: Option<JdbcDialect>) : Source, Logging {

  protected fun connect(): Connection {
    logger.info("Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug("Connected to $url")
    return conn
  }

  protected abstract fun fetchSchema(): Schema

  val schema: Schema = providedSchema.getOrElse { fetchSchema() }

  protected fun schemaFor(rs: ResultSet): Schema {
    val dialect = providedDialect.getOrElse { io.eels.component.jdbc.JdbcDialect(url) }
    val schema = JdbcSchemaFn(rs, dialect)
    logger.debug("Fetched schema:\n" + schema.print())
    return schema
  }
}
