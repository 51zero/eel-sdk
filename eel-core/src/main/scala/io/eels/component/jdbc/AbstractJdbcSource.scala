package io.eels.component.jdbc

import java.sql.{Connection, DriverManager, ResultSet}

import com.sksamuel.scalax.Logging
import io.eels.{Schema, Source}

abstract class AbstractJdbcSource(url: String,
                                  providedSchema: Option[Schema],
                                  providedDialect: Option[JdbcDialect]) extends Source with Logging {

  protected def connect(): Connection = {
    logger.info(s"Connecting to jdbc source $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")
    conn
  }

  protected def fetchSchema: Schema

  lazy val schema: Schema = providedSchema getOrElse fetchSchema

  protected def schemaFor(rs: ResultSet): Schema = {
    val dialect = providedDialect.getOrElse(JdbcDialect(url))
    val schema = JdbcSchemaFn(rs, dialect)
    logger.debug("Fetched schema:\n" + schema.print)
    schema
  }

}
