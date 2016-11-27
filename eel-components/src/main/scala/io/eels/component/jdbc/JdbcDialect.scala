package io.eels.component.jdbc

import io.eels.schema.{DataType, Field, StructType}
import io.eels.Row

trait JdbcDialect {

  def create(schema: StructType, table: String): String
  def insert(row: Row, table: String): String
  def toJdbcType(field: Field): String
  def fromJdbcType(i: Int): DataType

  /**
    * Returns a parameterized insert query
    */
  def insertQuery(schema: StructType, table: String): String
}

object JdbcDialect {
  def apply(url: String): JdbcDialect = new GenericJdbcDialect()
}

