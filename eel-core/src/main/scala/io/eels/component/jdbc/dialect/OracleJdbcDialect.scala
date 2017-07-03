package io.eels.component.jdbc.dialect

import java.sql.{ResultSetMetaData, Types}

import com.sksamuel.exts.Logging
import io.eels.component.jdbc.JdbcReaderConfig
import io.eels.schema.{DataType, DecimalType}

class OracleJdbcDialect extends GenericJdbcDialect with Logging {

  val config = JdbcReaderConfig()

  // oracle uses its own timestamp types
  override def sanitize(value: Any): Any = {
    value.getClass.getName match {
      case "oracle.sql.TIMESTAMP" => value.getClass.getDeclaredMethod("timestampValue").invoke(value)
      case other => super.sanitize(other)
    }
  }

  // http://stackoverflow.com/questions/593197/what-is-the-default-precision-and-scale-for-a-number-in-oracle
  private def decimalType(column: Int, metadata: ResultSetMetaData): DataType = {
    val precision = metadata.getPrecision(column)
    val scale = metadata.getScale(column)
    require(scale <= precision, "Scale must be less than precision")

    // if scale == -127 then it means no scale was defined, which is what happens when you do NUMBER
    // in oracle without a scale or precision, and then it defaults to max precision/max scale, but storing
    // the number as given. For eel we'll use the max precision hive supports.
    DecimalType(
      if (precision <= 0) config.defaultPrecision else precision,
      if (scale == -127) 18 else if (scale < 0) config.defaultScale else scale
    )
  }

  override def fromJdbcType(column: Int, metadata: ResultSetMetaData): DataType = {
    metadata.getColumnType(column) match {
      case Types.DECIMAL | Types.NUMERIC => decimalType(column, metadata)
      case other => super.fromJdbcType(column, metadata)
    }
  }
}