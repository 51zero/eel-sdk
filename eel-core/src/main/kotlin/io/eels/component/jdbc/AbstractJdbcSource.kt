package io.eels.component.jdbc

import io.eels.Logging
import io.eels.Schema
import io.eels.Source
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

abstract class AbstractJdbcSource(val url: String,
                                  val providedSchema: Schema?,
                                  val providedDialect: JdbcDialect?) : Source, Logging {

  protected fun connect(): Connection {
    logger.info("Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug("Connected to $url")
    return conn
  }

  protected abstract fun fetchSchema(): Schema

  val schema: Schema = providedSchema ?: fetchSchema()

  protected fun schemaFor(rs: ResultSet): Schema {
    val dialect = providedDialect ?: io.eels.component.jdbc.JdbcDialect(url)
    val schema = JdbcSchemaFn(rs, dialect)
    logger.debug("Fetched schema:\n" + schema.print())
    return schema
  }
}
