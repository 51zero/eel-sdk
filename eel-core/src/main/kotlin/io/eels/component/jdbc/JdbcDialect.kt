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

open class GenericJdbcDialect : JdbcDialect, Logging {

  override fun toJdbcType(field: Field): String = when (field.type) {
    FieldType.Long -> "int"
    FieldType.BigInt -> "int"
    FieldType.Int -> "int"
    FieldType.Short -> "smallint"
    FieldType.String -> {
      if (field.precision.value > 0) "varchar(${field.precision})"
      else "varchar(255)"
    }
    else -> {
      logger.warn("Unknown schema type ${field.type}")
      "varchar(255)"
    }
  }

  override fun fromJdbcType(i: Int): FieldType = when (i) {
    java.sql.Types.BIGINT -> FieldType.Long
    java.sql.Types.BINARY -> FieldType.Binary
    java.sql.Types.BIT -> FieldType.Boolean
    java.sql.Types.BLOB -> FieldType.Binary
    java.sql.Types.BOOLEAN -> FieldType.Boolean
    java.sql.Types.CHAR -> FieldType.String
    java.sql.Types.CLOB -> FieldType.String
    java.sql.Types.DATALINK -> throw UnsupportedOperationException()
    java.sql.Types.DATE -> FieldType.Date
    java.sql.Types.DECIMAL -> FieldType.Decimal
    java.sql.Types.DISTINCT -> throw UnsupportedOperationException()
    java.sql.Types.DOUBLE -> FieldType.Double
    java.sql.Types.FLOAT -> FieldType.Float
    java.sql.Types.INTEGER -> FieldType.Int
    java.sql.Types.JAVA_OBJECT -> FieldType.String
    java.sql.Types.LONGNVARCHAR -> FieldType.String
    java.sql.Types.LONGVARBINARY -> FieldType.Binary
    java.sql.Types.LONGVARCHAR -> FieldType.String
    java.sql.Types.NCHAR -> FieldType.String
    java.sql.Types.NCLOB -> FieldType.String
    java.sql.Types.NULL -> FieldType.String
    java.sql.Types.NUMERIC -> FieldType.Decimal
    java.sql.Types.NVARCHAR -> FieldType.String
    java.sql.Types.OTHER -> FieldType.String
    java.sql.Types.REAL -> FieldType.Double
    java.sql.Types.REF -> FieldType.String
    java.sql.Types.ROWID -> FieldType.Long
    java.sql.Types.SMALLINT -> FieldType.Int
    java.sql.Types.SQLXML -> FieldType.String
    java.sql.Types.STRUCT -> FieldType.String
    java.sql.Types.TIME -> FieldType.Timestamp
    java.sql.Types.TIMESTAMP -> FieldType.Timestamp
    java.sql.Types.TINYINT -> FieldType.Int
    java.sql.Types.VARBINARY -> FieldType.Binary
    java.sql.Types.VARCHAR -> FieldType.String
    else -> FieldType.String
  }

  override fun create(schema: Schema, table: String): String {
    val columns = schema.fields.map { "${it.name} ${toJdbcType(it)}" }.joinToString("(", ",", ")")
    return "CREATE TABLE $table $columns"
  }

  override fun insertQuery(schema: Schema, table: String): String {
    val columns = schema.columnNames().joinToString(",")
    val parameters = Array(schema.fields.size, { "?" }).joinToString(",")
    return "INSERT INTO $table ($columns) VALUES ($parameters)"
  }

  override fun insert(row: Row, table: String): String {
    // todo use proper statements
    val columns = row.schema.columnNames().joinToString(",")
    val values = row.values.joinToString("'", "','", "'")
    return "INSERT INTO $table ($columns) VALUES ($values)"
  }
}