package io.eels.component.parquet

import java.math.BigInteger
import java.time.{Instant, LocalDateTime, ZoneId}

import io.eels.schema._
import org.apache.parquet.io.api.{Binary, RecordConsumer}

// accepts a scala/java value and writes it out to a record consumer as the appropriate
// parquet value for the given schema type
trait ParquetValueConversion {
  def write(record: RecordConsumer, value: Any)
}

object ParquetValueConversion {
  def apply(dataType: DataType): ParquetValueConversion = {
    dataType match {
      case BinaryType => BinaryParquetWriter
      case BigIntType => BigIntParquetValueConversion
      case BooleanType => BooleanParquetValueConversion
      case DateType => DateParquetValueConversion
      case DecimalType(precision, scale) => new DecimalWriter(precision, scale)
      case DoubleType => DoubleParquetValueConversion
      case FloatType => FloatParquetValueConversion
      case _: IntType => IntParquetValueWriter
      case _: LongType => LongParquetValueWriter
      case _: ShortType => ShortParquetWriter
      case StringType => StringParquetValueConversion
      case TimeType => TimeParquetValueConversion
      case TimestampType => TimestampParquetValueConversion
    }
  }
}

object BinaryParquetWriter extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case array: Array[Byte] =>
        record.addBinary(Binary.fromReusedByteArray(value.asInstanceOf[Array[Byte]]))
      case seq: Seq[Byte] => write(record, seq.toArray)
    }
  }
}

// The scale stores the number of digits of that value that are to the right of the decimal point,
// and the precision stores the maximum number of digits supported in the unscaled value.
class DecimalWriter(precision: Precision, scale: Scale) extends ParquetValueConversion {

  override def write(record: RecordConsumer, value: Any): Unit = {
    val bi = value.asInstanceOf[BigDecimal].underlying().unscaledValue().multiply(BigInteger.valueOf(10).pow(precision.value))
    val bytes = bi.toByteArray
    record.addBinary(Binary.fromReusedByteArray(bytes))
  }
}

object BigIntParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(value.asInstanceOf[BigInt].toLong)
  }
}

object DateParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case date: java.sql.Date =>
        val dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneId.systemDefault)
        val days = dt.getDayOfYear + dt.getYear * 365
        record.addInteger(days)
    }
  }
}

object TimeParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(value.toString.toLong)
  }
}

object TimestampParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case timestamp: java.sql.Timestamp =>
        val dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime), ZoneId.systemDefault).plusNanos(timestamp.getNanos)
        val binary = Binary.fromReusedByteArray(Array[Byte](1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6))
        record.addBinary(binary)
    }
  }
}

object StringParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addBinary(Binary.fromString(value.toString))
  }
}

object ShortParquetWriter extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addInteger(value.toString.toShort)
  }
}

object DoubleParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addDouble(value.asInstanceOf[Double])
  }
}

object FloatParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addFloat(value.asInstanceOf[Float])
  }
}

object BooleanParquetValueConversion extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addBoolean(value.asInstanceOf[Boolean])
  }
}

object LongParquetValueWriter extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(value.asInstanceOf[Long])
  }
}

object IntParquetValueWriter extends ParquetValueConversion {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addInteger(value.asInstanceOf[Int])
  }
}