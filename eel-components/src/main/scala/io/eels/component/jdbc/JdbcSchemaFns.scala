package io.eels.component.jdbc

import java.sql.{ResultSet, ResultSetMetaData}

import com.sksamuel.exts.Logging
import io.eels.schema.{Field, StructType}

/**
 * Generates an eel schema from the metadata in a resultset.
 */
object JdbcSchemaFns extends Logging {

  def fromJdbcResultset(rs: ResultSet, dialect: JdbcDialect): StructType = {
    logger.debug("Building frame schema from resultset")

    val md = rs.getMetaData
    val columnCount = md.getColumnCount
    logger.debug(s"Resultset column count is $columnCount")

    val cols = (1 to columnCount).map { k =>
      Field(
          name = md.getColumnLabel(k),
          dataType = dialect.fromJdbcType(md.getColumnType(k)),
          nullable = md.isNullable(k) == ResultSetMetaData.columnNullable
      )
    }

    StructType(cols.toList)
  }
}
