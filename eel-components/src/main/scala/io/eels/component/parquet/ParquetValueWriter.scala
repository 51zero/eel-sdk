package io.eels.component.parquet

import java.nio.{ByteBuffer, ByteOrder}
import java.time._
import java.time.temporal.ChronoUnit

import io.eels.coercion.{BigDecimalCoercer, DoubleCoercer}
import io.eels.schema._
import org.apache.parquet.io.api.{Binary, RecordConsumer}

// accepts a scala/java value and writes it out to a record consumer as the appropriate
// parquet value for the given schema type
trait ParquetValueWriter {
  def write(record: RecordConsumer, value: Any)
}

object ParquetValueWriter {
  def apply(dataType: DataType): ParquetValueWriter = {
    dataType match {
      case BinaryType => BinaryParquetWriter
      case BigIntType => BigIntParquetValueWriter
      case BooleanType => BooleanParquetValueWriter
      case CharType(_) => StringParquetValueWriter
      case DateType => DateParquetValueWriter
      case DecimalType(precision, scale) => new DecimalWriter(precision, scale)
      case DoubleType => DoubleParquetValueWriter
      case FloatType => FloatParquetValueWriter
      case _: IntType => IntParquetValueWriter
      case _: LongType => LongParquetValueWriter
      case _: ShortType => ShortParquetWriter
      case StringType => StringParquetValueWriter
      case struct: StructType => new StructWriter(struct, true)
      case TimeMillisType => TimeParquetValueWriter
      case TimestampMillisType => TimestampParquetValueWriter
      case VarcharType(_) => StringParquetValueWriter
    }
  }
}

class StructWriter(structType: StructType, group: Boolean) extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    require(record != null)
    if (group)
      record.startGroup()
    val values = value.asInstanceOf[Seq[Any]]
    for (k <- structType.fields.indices) {
      val value = values(k)
      // if a value is null then parquet requires us to completely skip the field
      if (value != null) {
        val field = structType.field(k)
        record.startField(field.name, k)
        val writer = ParquetValueWriter(field.dataType)
        writer.write(record, value)
        record.endField(field.name, k)
      }
    }
    if (group)
      record.endGroup()
  }
}

object BinaryParquetWriter extends ParquetValueWriter {
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
class DecimalWriter(precision: Precision, scale: Scale) extends ParquetValueWriter {

  val bits = ParquetSchemaFns.byteSizeForPrecision(precision.value)

  override def write(record: RecordConsumer, value: Any): Unit = {
    val bd = BigDecimalCoercer.coerce(value)
      .setScale(scale.value)
      .underlying()
      .unscaledValue()
    val padded = bd.toByteArray.reverse.padTo(bits, 0: Byte).reverse
    record.addBinary(Binary.fromReusedByteArray(padded))
  }
}

object BigIntParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(value.asInstanceOf[BigInt].toLong)
  }
}

object DateParquetValueWriter extends ParquetValueWriter {

  val UnixEpoch = LocalDate.of(1970, 1, 1)

  // should write out number of days since unix epoch
  override def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case date: java.sql.Date =>
        val local = Instant.ofEpochMilli(date.getTime).atZone(ZoneId.systemDefault).toLocalDate
        val days = ChronoUnit.DAYS.between(UnixEpoch, local)
        record.addInteger(days.toInt)
    }
  }
}


object TimeParquetValueWriter extends ParquetValueWriter {

  val JulianEpochInGregorian = LocalDateTime.of(-4713, 11, 24, 0, 0, 0)

  // first 8 bytes are the nanoseconds
  // second 4 bytes are the days
  override def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case timestamp: java.sql.Timestamp =>
        val nanos = timestamp.getNanos
        val dt = Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.systemDefault)
        val days = ChronoUnit.DAYS.between(JulianEpochInGregorian, dt).toInt
        val bytes = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(days)
        val binary = Binary.fromReusedByteBuffer(bytes)
        record.addBinary(binary)
    }
  }
}

object TimestampParquetValueWriter extends ParquetValueWriter {

  val JulianEpochInGregorian = LocalDateTime.of(-4713, 11, 24, 0, 0, 0)

  override def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case timestamp: java.sql.Timestamp =>
        val dt = Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.systemDefault)
        val days = ChronoUnit.DAYS.between(JulianEpochInGregorian, dt).toInt
        val nanos = timestamp.getNanos + ChronoUnit.NANOS.between(dt.toLocalDate.atStartOfDay(ZoneId.systemDefault).toLocalTime, dt.toLocalTime)
        val bytes = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(days).array()
        val binary = Binary.fromReusedByteArray(bytes)
        record.addBinary(binary)
    }
  }
}

object StringParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addBinary(Binary.fromString(value.toString))
  }
}

object ShortParquetWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addInteger(value.toString.toShort)
  }
}

object DoubleParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addDouble(DoubleCoercer.coerce(value))
  }
}

object FloatParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addFloat(value.asInstanceOf[Float])
  }
}

object BooleanParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addBoolean(value.asInstanceOf[Boolean])
  }
}

object LongParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(value.asInstanceOf[Long])
  }
}

object IntParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addInteger(value.asInstanceOf[Int])
  }
}