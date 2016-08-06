package io.eels.component.jdbc

import io.eels.schema.Schema
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

import com.sksamuel.exts.Logging

trait JdbcPrimitives extends Logging {

  def connect(url: String): Connection = {
    logger.info("Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug("Connected to $url")
    conn
  }

  def schemaFor(dialect: JdbcDialect, rs: ResultSet): Schema = {
    val schema = JdbcSchemaFns.fromJdbcResultset(rs, dialect)
    logger.debug("Fetched schema:\n" + schema.show())
    schema
  }
}
