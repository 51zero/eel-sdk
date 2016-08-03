package io.eels.component.jdbc

import io.eels.schema.Schema
import io.eels.util.Logging
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

interface JdbcPrimitives : Logging {

  fun connect(url: String): Connection {
    logger.info("Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug("Connected to $url")
    return conn
  }

  fun schemaFor(dialect: JdbcDialect, rs: ResultSet): Schema {
    val schema = JdbcSchemaFn(rs, dialect)
    logger.debug("Fetched schema:\n" + schema.show())
    return schema
  }
}
