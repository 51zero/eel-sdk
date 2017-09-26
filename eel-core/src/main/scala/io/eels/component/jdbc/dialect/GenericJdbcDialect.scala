package io.eels.component.jdbc.dialect

import java.sql.{ResultSetMetaData, Types}

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.schema._

class GenericJdbcDialect extends JdbcDialect with Logging {

  // generic jdbc will just return the values as is
  override def sanitize(value: Any): Any = value

  override def toJdbcType(field: Field): String = field.dataType match {
    case BigIntType => "int"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case CharType(size) => s"char($size)"
    case DateType => "date"
    case DecimalType(precision, scale) => s"decimal(${precision.value}, ${scale.value})"
    case DoubleType => "double"
    case FloatType => "float"
    case EnumType(_, _) => "varchar(255)"
    case IntType(_) => "int"
    case LongType(_) => "bigint"
    case ShortType(_) => "smallint"
    case StringType => "text"
    case TimestampMillisType => "timestamp"
    case TimestampMicrosType => sys.error("Not supported by JDBC")
    case VarcharType(size) =>
      if (size > 0) s"varchar($size)"
      else {
        logger.warn(s"Invalid size $size specified for varchar; defaulting to 255")
        "varchar(255)"
      }
    case _ => sys.error(s"Unsupported data type with JDBC Sink: ${field.dataType}")
  }

  private def decimalType(column: Int, metadata: ResultSetMetaData): DecimalType = {
    val precision = metadata.getPrecision(column)
    val scale = metadata.getScale(column)
    require(scale <= precision, "Scale must be less than precision")
    DecimalType(precision, scale)
  }

  override def fromJdbcType(column: Int, metadata: ResultSetMetaData): DataType = {
    metadata.getColumnType(column) match {
      case Types.BIGINT => LongType.Signed
      case Types.BINARY => BinaryType
      case Types.BIT => BooleanType
      case Types.BLOB => BinaryType
      case Types.BOOLEAN => BooleanType
      case Types.CHAR => CharType(metadata.getPrecision(column))
      case Types.CLOB => StringType
      case Types.DATALINK => throw new UnsupportedOperationException()
      case Types.DATE => DateType
      case Types.DECIMAL => decimalType(column, metadata)
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
      case Types.NUMERIC => decimalType(column, metadata)
      case Types.NVARCHAR => StringType
      case Types.OTHER => StringType
      case Types.REAL => DoubleType
      case Types.REF => StringType
      case Types.ROWID => LongType.Signed
      case Types.SMALLINT => ShortType.Signed
      case Types.SQLXML => StringType
      case Types.STRUCT => StringType
      case Types.TIME => TimeMillisType
      case Types.TIMESTAMP => TimestampMillisType
      case Types.TINYINT => ShortType.Signed
      case Types.VARBINARY => BinaryType
      case Types.VARCHAR => VarcharType(metadata.getPrecision(column))
      case other =>
        logger.warn(s"Unknown jdbc type $other; defaulting to StringType")
        StringType
    }
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