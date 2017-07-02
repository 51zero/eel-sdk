package io.eels.component.jdbc.dialect

import io.eels.Row
import io.eels.schema.{DataType, Field, StructType}

trait JdbcDialect {

  def create(schema: StructType, table: String): String
  def insert(row: Row, table: String): String
  def toJdbcType(field: Field): String
  def fromJdbcType(column: Int, metadata: java.sql.ResultSetMetaData): DataType

  /**
    * Returns a parameterized insert query
    */
  def insertQuery(schema: StructType, table: String): String
}

object JdbcDialect {
  def apply(url: String): JdbcDialect = {
    if (url.toLowerCase.startsWith("jdbc:oracle")) new OracleJdbcDialect()
    else new GenericJdbcDialect()
  }
}

