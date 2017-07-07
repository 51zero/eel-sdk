package io.eels.component.jdbc.dialect

import java.sql.{ResultSetMetaData, Types}

import com.sksamuel.exts.Logging
import io.eels.component.jdbc.JdbcReaderConfig
import io.eels.schema.{BooleanType, ByteType, DataType, DecimalType, DoubleType, Field, FloatType, IntType, LongType, Precision, Scale, ShortType}

class OracleJdbcDialect extends GenericJdbcDialect with Logging {

  private val config = JdbcReaderConfig()

  // oracle uses its own timestamp types
  override def sanitize(value: Any): Any = {
    if (value == null) null
    else value match {
      case other if other.getClass.getName == "oracle.sql.TIMESTAMP" =>
        value.getClass.getDeclaredMethod("timestampValue").invoke(value)
      case other => super.sanitize(other)
    }
  }

  override def toJdbcType(field: Field): String = field.dataType match {
    // https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    case BooleanType => "NUMBER(1)"
    case IntType(_) => "NUMBER(10)"
    case LongType(_) => "NUMBER(19)"
    case FloatType => "NUMBER(19, 4)"
    case DoubleType => "NUMBER(19, 4)"
    case ByteType(_) => "NUMBER(3)"
    case ShortType(_) => "NUMBER(5)"
    case _ => super.toJdbcType(field)
  }

  private def decimalType(column: Int, metadata: ResultSetMetaData): DataType = {
    val precision = metadata.getPrecision(column)
    val scale = metadata.getScale(column)
    require(scale <= precision, "Scale must be less than precision")

    precision match {
      // Jdbc returns precision == 0 and scale == -127 for NUMBER fields which have no precision/scale
      // http://stackoverflow.com/questions/593197/what-is-the-default-precision-and-scale-for-a-number-in-oracle
      case 0 => DecimalType(config.defaultPrecision, config.defaultScale)
      case _ if scale == -127L => DecimalType(config.defaultPrecision, config.defaultScale)
      case _ => DecimalType(Precision(precision), Scale(scale))
    }
  }

  override def fromJdbcType(column: Int, metadata: ResultSetMetaData): DataType = {
    metadata.getColumnType(column) match {
      case Types.DECIMAL | Types.NUMERIC => decimalType(column, metadata)
      case _ => super.fromJdbcType(column, metadata)
    }
  }
}