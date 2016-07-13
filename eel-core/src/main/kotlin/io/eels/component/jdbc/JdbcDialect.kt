package io.eels.component.jdbc

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.util.Logging
import io.eels.Row
import io.eels.schema.Schema

interface JdbcDialect {

  fun create(schema: Schema, table: String): String
  fun insert(row: Row, table: String): String
  fun toJdbcType(field: Field): String
  fun fromJdbcType(i: Int): FieldType

  /**
    * Returns a parameterized insert query
    */
  fun insertQuery(schema: Schema, table: String): String

  companion object {
    operator fun invoke(url: String): JdbcDialect = GenericJdbcDialect()
  }
}

