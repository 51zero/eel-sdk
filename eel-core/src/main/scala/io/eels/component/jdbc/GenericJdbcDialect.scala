package io.eels.component.jdbc

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import java.sql.Types

import com.sksamuel.exts.Logging

class GenericJdbcDialect extends JdbcDialect with Logging {

  override def toJdbcType(field: Field): String = field.`type` match {
    case FieldType.Long => "int"
    case FieldType.BigInt => "int"
    case FieldType.Int => "int"
    case FieldType.Short => "smallint"
    case FieldType.Boolean => "BOOLEAN"
    case FieldType.String =>
      if (field.precision.value > 0) s"varchar(${field.precision.value})"
      else "varchar(255)"
    case _ =>
      logger.warn(s"Unknown schema type ${field.`type`}")
      "varchar(255)"
    }

  override def fromJdbcType(i: Int): FieldType = i match {
    case Types.BIGINT => FieldType.Long
    case Types.BINARY => FieldType.Binary
    case Types.BIT => FieldType.Boolean
    case Types.BLOB => FieldType.Binary
    case Types.BOOLEAN => FieldType.Boolean
    case Types.CHAR => FieldType.String
    case Types.CLOB => FieldType.String
    case Types.DATALINK => throw new UnsupportedOperationException()
    case Types.DATE => FieldType.Date
    case Types.DECIMAL => FieldType.Decimal
    case Types.DISTINCT => throw new UnsupportedOperationException()
    case Types.DOUBLE => FieldType.Double
    case Types.FLOAT => FieldType.Float
    case Types.INTEGER => FieldType.Int
    case Types.JAVA_OBJECT => FieldType.String
    case Types.LONGNVARCHAR => FieldType.String
    case Types.LONGVARBINARY => FieldType.Binary
    case Types.LONGVARCHAR => FieldType.String
    case Types.NCHAR => FieldType.String
    case Types.NCLOB => FieldType.String
    case Types.NULL => FieldType.String
    case Types.NUMERIC => FieldType.Decimal
    case Types.NVARCHAR => FieldType.String
    case Types.OTHER => FieldType.String
    case Types.REAL => FieldType.Double
    case Types.REF => FieldType.String
    case Types.ROWID => FieldType.Long
    case Types.SMALLINT => FieldType.Int
    case Types.SQLXML => FieldType.String
    case Types.STRUCT => FieldType.String
    case Types.TIME => FieldType.Timestamp
    case Types.TIMESTAMP => FieldType.Timestamp
    case Types.TINYINT => FieldType.Int
    case Types.VARBINARY => FieldType.Binary
    case Types.VARCHAR => FieldType.String
    case _ => FieldType.String
  }

  override def create(schema: Schema, table: String): String = {
    val columns = schema.fields.map { it => s"${it.name} ${toJdbcType(it)}" }.mkString(",", "(", ")")
    s"CREATE TABLE $table $columns"
  }

  override def insertQuery(schema: Schema, table: String): String = {
    val columns = schema.fieldNames().mkString(",")
    val parameters = List.fill(schema.fields.size)("?").mkString(",")
    s"INSERT INTO $table ($columns) VALUES ($parameters)"
  }

  override def insert(row: Row, table: String): String = {
    // todo use proper statements
    val columns = row.schema.fieldNames().mkString(",")
    val values = row.values.mkString("'", "','", "'")
    s"INSERT INTO $table ($columns) VALUES ($values)"
  }
}