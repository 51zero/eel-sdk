package io.eels.component.jdbc

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import io.eels.util.Logging
import java.sql.Types

open class GenericJdbcDialect : JdbcDialect, Logging {

  override fun toJdbcType(field: Field): String = when (field.type) {
    FieldType.Long -> "int"
    FieldType.BigInt -> "int"
    FieldType.Int -> "int"
    FieldType.Short -> "smallint"
    FieldType.Boolean -> "BOOLEAN"
    FieldType.String -> {
      if (field.precision.value > 0) "varchar(${field.precision.value})"
      else "varchar(255)"
    }
    else -> {
      logger.warn("Unknown schema type ${field.type}")
      "varchar(255)"
    }
  }

  override fun fromJdbcType(i: Int): FieldType = when (i) {
    Types.BIGINT -> FieldType.Long
    Types.BINARY -> FieldType.Binary
    Types.BIT -> FieldType.Boolean
    Types.BLOB -> FieldType.Binary
    Types.BOOLEAN -> FieldType.Boolean
    Types.CHAR -> FieldType.String
    Types.CLOB -> FieldType.String
    Types.DATALINK -> throw UnsupportedOperationException()
    Types.DATE -> FieldType.Date
    Types.DECIMAL -> FieldType.Decimal
    Types.DISTINCT -> throw UnsupportedOperationException()
    Types.DOUBLE -> FieldType.Double
    Types.FLOAT -> FieldType.Float
    Types.INTEGER -> FieldType.Int
    Types.JAVA_OBJECT -> FieldType.String
    Types.LONGNVARCHAR -> FieldType.String
    Types.LONGVARBINARY -> FieldType.Binary
    Types.LONGVARCHAR -> FieldType.String
    Types.NCHAR -> FieldType.String
    Types.NCLOB -> FieldType.String
    Types.NULL -> FieldType.String
    Types.NUMERIC -> FieldType.Decimal
    Types.NVARCHAR -> FieldType.String
    Types.OTHER -> FieldType.String
    Types.REAL -> FieldType.Double
    Types.REF -> FieldType.String
    Types.ROWID -> FieldType.Long
    Types.SMALLINT -> FieldType.Int
    Types.SQLXML -> FieldType.String
    Types.STRUCT -> FieldType.String
    Types.TIME -> FieldType.Timestamp
    Types.TIMESTAMP -> FieldType.Timestamp
    Types.TINYINT -> FieldType.Int
    Types.VARBINARY -> FieldType.Binary
    Types.VARCHAR -> FieldType.String
    else -> FieldType.String
  }

  override fun create(schema: Schema, table: String): String {
    val columns = schema.fields.map { "${it.name} ${toJdbcType(it)}" }.joinToString(",", "(", ")")
    return "CREATE TABLE $table $columns"
  }

  override fun insertQuery(schema: Schema, table: String): String {
    val columns = schema.fieldNames().joinToString(",")
    val parameters = Array(schema.fields.size, { "?" }).joinToString(",")
    return "INSERT INTO $table ($columns) VALUES ($parameters)"
  }

  override fun insert(row: Row, table: String): String {
    // todo use proper statements
    val columns = row.schema.fieldNames().joinToString(",")
    val values = row.values.joinToString("'", "','", "'")
    return "INSERT INTO $table ($columns) VALUES ($values)"
  }
}