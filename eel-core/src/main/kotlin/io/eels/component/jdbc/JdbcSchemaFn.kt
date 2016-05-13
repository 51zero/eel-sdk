package io.eels.component.jdbc

import io.eels.Column
import io.eels.Logging
import io.eels.Schema
import java.sql.ResultSet
import java.sql.ResultSetMetaData

/**
 * Generates an eel schema from the metadata in a resultset.
 */
object JdbcSchemaFn : Logging {

  operator fun invoke(rs: ResultSet, dialect: JdbcDialect): Schema {
    logger.debug("Building frame schema from resultset")

    val md = rs.metaData
    val columnCount = md.columnCount
    logger.debug("Resultset column count is $columnCount")

    val cols = (1..columnCount).map { k ->
      Column(
          name = md.getColumnLabel(k),
          `type` = dialect.fromJdbcType(md.getColumnType(k)),
          nullable = md.isNullable(k) == ResultSetMetaData.columnNullable,
          precision = md.getPrecision(k),
          scale = md.getScale(k),
          signed = md.isSigned(k)
      )
    }

    return Schema(cols)
  }
}
