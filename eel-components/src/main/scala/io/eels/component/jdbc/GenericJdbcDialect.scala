package io.eels.component.jdbc

import io.eels.Row
import io.eels.schema._
import java.sql.Types

import com.sksamuel.exts.Logging

class GenericJdbcDialect extends JdbcDialect with Logging {

  override def toJdbcType(field: Field): String = field.dataType match {
    case LongType(_) => "int"
    case BigIntType => "int"
    case IntType(_) => "int"
    case ShortType(_) => "smallint"
    case BooleanType => "BOOLEAN"
    case StringType => "text"
    case VarcharType(size) =>
      if (size > 0) s"varchar($size)"
      else "varchar(255)"
    case _ =>
      logger.warn(s"Unknown data type ${field.dataType}")
      "varchar(255)"
    }

  override def fromJdbcType(i: Int): DataType = i match {
    case Types.BIGINT => BigIntType
    case Types.BINARY => BinaryType
    case Types.BIT => BooleanType
    case Types.BLOB => BinaryType
    case Types.BOOLEAN => BooleanType
    case Types.CHAR => CharType(255)
    case Types.CLOB => StringType
    case Types.DATALINK => throw new UnsupportedOperationException()
    case Types.DATE => DateType
    case Types.DECIMAL => DecimalType()
    case Types.DISTINCT => throw new UnsupportedOperationException()
    case Types.DOUBLE => DoubleType
    case Types.FLOAT => FloatType
    case Types.INTEGER => IntType.Signed
    case Types.JAVA_OBJECT => BinaryType
    case Types.LONGNVARCHAR => StringType
    case Types.LONGVARBINARY => BinaryType
    case Types.LONGVARCHAR => StringType
    case Types.NCHAR => StringType
    case Types.NCLOB => StringType
    case Types.NULL => StringType
    case Types.NUMERIC => DecimalType.Default
    case Types.NVARCHAR => StringType
    case Types.OTHER => StringType
    case Types.REAL => DoubleType
    case Types.REF => StringType
    case Types.ROWID => LongType.Signed
    case Types.SMALLINT => ShortType.Signed
    case Types.SQLXML => StringType
    case Types.STRUCT => StringType
    case Types.TIME => TimeType
    case Types.TIMESTAMP => TimestampType
    case Types.TINYINT => ShortType.Signed
    case Types.VARBINARY => BinaryType
    case Types.VARCHAR => StringType
    case _ => StringType
  }

  override def create(schema: StructType, table: String): String = {
    val columns = schema.fields.map { it => s"${it.name} ${toJdbcType(it)}" }.mkString("(", ",", ")")
    s"CREATE TABLE $table $columns"
  }

  override def insertQuery(schema: StructType, table: String): String = {
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