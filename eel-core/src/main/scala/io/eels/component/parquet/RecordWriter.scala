package io.eels.component.parquet

import java.nio.{ByteBuffer, ByteOrder}
import java.time._
import java.time.temporal.ChronoUnit

import com.sksamuel.exts.Logging
import io.eels.coercion._
import io.eels.schema._
import org.apache.parquet.io.api.{Binary, RecordConsumer}

import scala.math.BigDecimal.RoundingMode.RoundingMode

// accepts a scala/java value and writes it out to a record consumer as
// the appropriate parquet type
trait RecordWriter {
  def _write(record: RecordConsumer, value: Any): Unit

  def write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case Some(someValue) => _write(record, someValue)
      case _ => _write(record, value)
    }
  }
}

object RecordWriter {
  def apply(dataType: DataType, roundingMode: RoundingMode): RecordWriter = {
    dataType match {
      case ArrayType(elementType) => new ArrayRecordWriter(RecordWriter(elementType, roundingMode))
      case BinaryType => BinaryParquetWriter
      case BigIntType => BigIntRecordWriter
      case BooleanType => BooleanRecordWriter
      case CharType(_) => StringRecordWriter
      case DateType => DateRecordWriter
      case DecimalType(precision, scale) => new DecimalWriter(precision, scale, roundingMode)
      case DoubleType => DoubleRecordWriter
      case FloatType => FloatRecordWriter
      case _: IntType => IntRecordWriter
      case _: LongType => LongRecordWriter
      case _: ShortType => ShortParquetWriter
      case mapType@MapType(keyType, valueType) =>
        new MapRecordWriter(mapType, apply(keyType, roundingMode), apply(valueType, roundingMode))
      case StringType => StringRecordWriter
      case struct: StructType => new StructRecordWriter(struct, roundingMode, true)
      case TimeMillisType => TimeRecordWriter
      case TimestampMillisType => TimestampRecordWriter
      case VarcharType(_) => StringRecordWriter
    }
  }
}

class MapRecordWriter(mapType: MapType,
                      keyWriter: RecordWriter,
                      valueWriter: RecordWriter) extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    val map = MapCoercer.coerce(value)

    record.startGroup()
    record.startField("key_value", 0)

    map.foreach { case (key, v) =>
      record.startGroup()
      record.startField("key", 0)
      keyWriter.write(record, key)
      record.endField("key", 0)

      record.startField("value", 1)
      valueWriter.write(record, v)
      record.endField("value", 1)
      record.endGroup()
    }

    record.endField("key_value", 0)
    record.endGroup()
  }
}

class ArrayRecordWriter(nested: RecordWriter) extends RecordWriter with Logging {
  override def _write(record: RecordConsumer, value: Any): Unit = {

    val seq = SequenceCoercer.coerce(value)

    // this layout follows the spark style, an array is a group of a single element called list, which itself
    // contains repeated groups which contain another record called element
    record.startGroup()
    record.startField("list", 0)

    seq.foreach { x =>
      record.startGroup()
      record.startField("element", 0)
      nested.write(record, x)
      record.endField("element", 0)
      record.endGroup()
    }

    record.endField("list", 0)
    record.endGroup()
  }
}

class StructRecordWriter(structType: StructType,
                         roundingMode: RoundingMode,
                         nested: Boolean // nested groups, ie not the outer record, must be handled differently
                        ) extends RecordWriter with Logging {

  val writers = structType.fields.map(_.dataType).map(RecordWriter.apply(_, roundingMode))

  override def _write(record: RecordConsumer, value: Any): Unit = {
    require(record != null)
    if (nested)
      record.startGroup()
    val values = SequenceCoercer.coerce(value)
    for (k <- structType.fields.indices) {
      val value = values(k)
      // if a value is null then parquet requires us to completely skip the field
      if (value != null) {
        val field = structType.field(k)
        record.startField(field.name, k)
        val writer = writers(k)
        writer.write(record, value)
        record.endField(field.name, k)
      }
    }
    if (nested)
      record.endGroup()
  }
}

object BinaryParquetWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case array: Array[Byte] => record.addBinary(Binary.fromReusedByteArray(array))
      case seq: Seq[Byte] => write(record, seq.toArray)
    }
  }
}

// The scale stores the number of digits of that value that are to the right of the decimal point,
// and the precision stores the maximum number of sig digits supported in the unscaled value.
class DecimalWriter(precision: Precision, scale: Scale, roundingMode: RoundingMode) extends RecordWriter {

  private val byteSizeForPrecision = ParquetSchemaFns.byteSizeForPrecision(precision.value)

  override def _write(record: RecordConsumer, value: Any): Unit = {
    val bd = BigDecimalCoercer.coerce(value)
      .setScale(scale.value, roundingMode)
      .underlying()
    record.addBinary(decimalAsBinary(bd, bd.unscaledValue()))
  }

  import org.apache.parquet.io.api.Binary

  private def decimalAsBinary(original: java.math.BigDecimal, unscaled: java.math.BigInteger): Binary = {
    val bytes = unscaled.toByteArray
    if (bytes.length == byteSizeForPrecision) Binary.fromReusedByteArray(bytes)
    else if (bytes.length < byteSizeForPrecision) {
      val decimalBuffer = new Array[Byte](byteSizeForPrecision)
      // For negatives all high bits need to be 1 hence -1 used
      val signByte = if (unscaled.signum < 0) -1: Byte else 0: Byte
      java.util.Arrays.fill(decimalBuffer, 0, decimalBuffer.length - bytes.length, signByte)
      System.arraycopy(bytes, 0, decimalBuffer, decimalBuffer.length - bytes.length, bytes.length)
      Binary.fromReusedByteArray(decimalBuffer)
    } else throw new IllegalStateException(s"Decimal precision too small, value=$original, precision=${precision.value}")
  }

}

object BigIntRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(BigIntegerCoercer.coerce(value).longValue)
  }
}

object DateRecordWriter extends RecordWriter {

  private val UnixEpoch = LocalDate.of(1970, 1, 1)

  // should write out number of days since unix epoch
  override def _write(record: RecordConsumer, value: Any): Unit = {
    value match {
      case date: java.sql.Date =>
        val local = Instant.ofEpochMilli(date.getTime).atZone(ZoneId.systemDefault).toLocalDate
        val days = ChronoUnit.DAYS.between(UnixEpoch, local)
        record.addInteger(days.toInt)
    }
  }
}


object TimeRecordWriter extends RecordWriter {

  private val JulianEpochInGregorian = LocalDateTime.of(-4713, 11, 24, 0, 0, 0)

  // first 8 bytes are the nanoseconds
  // second 4 bytes are the days
  override def _write(record: RecordConsumer, value: Any): Unit = {
    val timestamp = TimestampCoercer.coerce(value)
    val nanos = timestamp.getNanos
    val dt = Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.systemDefault)
    val days = ChronoUnit.DAYS.between(JulianEpochInGregorian, dt).toInt
    val bytes = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(days)
    val binary = Binary.fromReusedByteBuffer(bytes)
    record.addBinary(binary)
  }
}

object TimestampRecordWriter extends RecordWriter {

  private val JulianEpochInGregorian = LocalDateTime.of(-4713, 11, 24, 0, 0, 0)

  override def _write(record: RecordConsumer, value: Any): Unit = {
    val timestamp = TimestampCoercer.coerce(value)
    val dt = Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.systemDefault)
    val days = ChronoUnit.DAYS.between(JulianEpochInGregorian, dt).toInt
    val nanos = timestamp.getNanos + ChronoUnit.NANOS.between(dt.toLocalDate.atStartOfDay(ZoneId.systemDefault).toLocalTime, dt.toLocalTime)
    val bytes = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(days).array()
    val binary = Binary.fromReusedByteArray(bytes)
    record.addBinary(binary)
  }
}

object StringRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addBinary(Binary.fromString(StringCoercer.coerce(value)))
  }
}

object ShortParquetWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addInteger(ShortCoercer.coerce(value))
  }
}

object DoubleRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addDouble(DoubleCoercer.coerce(value))
  }
}

object FloatRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addFloat(FloatCoercer.coerce(value))
  }
}

object BooleanRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addBoolean(BooleanCoercer.coerce(value))
  }
}

object LongRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addLong(LongCoercer.coerce(value))
  }
}

object IntRecordWriter extends RecordWriter {
  override def _write(record: RecordConsumer, value: Any): Unit = {
    record.addInteger(IntCoercer.coerce(value))
  }
}