package io.eels.component.jdbc

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.Row
import io.eels.schema.Schema

trait JdbcDialect {

  def create(schema: Schema, table: String): String
  def insert(row: Row, table: String): String
  def toJdbcType(field: Field): String
  def fromJdbcType(i: Int): FieldType

  /**
    * Returns a parameterized insert query
    */
  def insertQuery(schema: Schema, table: String): String
}

object JdbcDialect {
  def apply(url: String): JdbcDialect = new GenericJdbcDialect()
}

