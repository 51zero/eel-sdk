package io.eels.component.jdbc

import io.eels.Column
import io.eels.ColumnType
import io.eels.Logging
import io.eels.Row
import io.eels.Schema

interface JdbcDialect {

  fun create(schema: Schema, table: String): String
  fun insert(row: Row, table: String): String
  fun toJdbcType(column: Column): String
  fun fromJdbcType(i: Int): ColumnType

  /**
    * Returns a parameterized insert query
    */
  fun insertQuery(schema: Schema, table: String): String

  companion object {
    operator fun invoke(url: String): JdbcDialect = GenericJdbcDialect()
  }
}

open class GenericJdbcDialect : JdbcDialect, Logging {

  override fun toJdbcType(column: Column): String = when (column.type) {
    ColumnType.Long -> "int"
    ColumnType.BigInt -> "int"
    ColumnType.Int -> "int"
    ColumnType.Short -> "smallint"
    ColumnType.String -> {
      if (column.precision > 0) "varchar(${column.precision})"
      else "varchar(255)"
    }
    else -> {
      logger.warn("Unknown schema type ${column.type}")
      "varchar(255)"
    }
  }

  override fun fromJdbcType(i: Int): ColumnType = when (i) {
    java.sql.Types.BIGINT -> ColumnType.Long
    java.sql.Types.BINARY -> ColumnType.Binary
    java.sql.Types.BIT -> ColumnType.Boolean
    java.sql.Types.BLOB -> ColumnType.Binary
    java.sql.Types.BOOLEAN -> ColumnType.Boolean
    java.sql.Types.CHAR -> ColumnType.String
    java.sql.Types.CLOB -> ColumnType.String
    java.sql.Types.DATALINK -> ColumnType.Unsupported
    java.sql.Types.DATE -> ColumnType.Date
    java.sql.Types.DECIMAL -> ColumnType.Decimal
    java.sql.Types.DISTINCT -> ColumnType.Unsupported
    java.sql.Types.DOUBLE -> ColumnType.Double
    java.sql.Types.FLOAT -> ColumnType.Float
    java.sql.Types.INTEGER -> ColumnType.Int
    java.sql.Types.JAVA_OBJECT -> ColumnType.Unsupported
    java.sql.Types.LONGNVARCHAR -> ColumnType.String
    java.sql.Types.LONGVARBINARY -> ColumnType.Binary
    java.sql.Types.LONGVARCHAR -> ColumnType.String
    java.sql.Types.NCHAR -> ColumnType.String
    java.sql.Types.NCLOB -> ColumnType.String
    java.sql.Types.NULL -> ColumnType.Unsupported
    java.sql.Types.NUMERIC -> ColumnType.Decimal
    java.sql.Types.NVARCHAR -> ColumnType.String
    java.sql.Types.OTHER -> ColumnType.Unsupported
    java.sql.Types.REAL -> ColumnType.Double
    java.sql.Types.REF -> ColumnType.String
    java.sql.Types.ROWID -> ColumnType.Long
    java.sql.Types.SMALLINT -> ColumnType.Int
    java.sql.Types.SQLXML -> ColumnType.String
    java.sql.Types.STRUCT -> ColumnType.String
    java.sql.Types.TIME -> ColumnType.Timestamp
    java.sql.Types.TIMESTAMP -> ColumnType.Timestamp
    java.sql.Types.TINYINT -> ColumnType.Int
    java.sql.Types.VARBINARY -> ColumnType.Binary
    java.sql.Types.VARCHAR -> ColumnType.String
    else -> ColumnType.Unsupported
  }

  override fun create(schema: Schema, table: String): String {
    val columns = schema.columns.map { "${it.name} ${toJdbcType(it)}" }.joinToString("(", ",", ")")
    return "CREATE TABLE $table $columns"
  }

  override fun insertQuery(schema: Schema, table: String): String {
    val columns = schema.columnNames().joinToString(",")
    val parameters = Array(schema.columns.size, { "?" }).joinToString(",")
    return "INSERT INTO $table ($columns) VALUES ($parameters)"
  }

  override fun insert(row: Row, table: String): String {
    // todo use proper statements
    val columns = row.schema.columnNames().joinToString(",")
    val values = row.values.joinToString("'", "','", "'")
    return "INSERT INTO $table ($columns) VALUES ($values)"
  }
}