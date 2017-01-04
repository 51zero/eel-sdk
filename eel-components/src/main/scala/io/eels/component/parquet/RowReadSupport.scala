package io.eels.component.parquet

import java.math.{BigInteger, MathContext}
import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.time.{LocalDateTime, ZoneId}
import java.util

import io.eels.schema._
import io.eels.{Row, RowBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType

// required by the parquet reader builder, and returns a record materializer for rows
class RowReadSupport extends ReadSupport[Row] {

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadContext): RecordMaterializer[Row] = {
    new RowMaterializer(fileSchema, readContext)
  }

  override def init(configuration: Configuration,
                    keyValueMetaData: util.Map[String, String],
                    fileSchema: MessageType): ReadSupport.ReadContext = {
    val partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA)
    val requestedProjection = ReadSupport.getSchemaForRead(fileSchema, partialSchemaString)
    new ReadSupport.ReadContext(requestedProjection)
  }
}

// a row materializer retrns a group converter which is invoked for each
// field in a group to get a converter for that field, and then each of those
// converts is in turn called with the basic value.
// The converter must know what to do with the basic value so where basic values
// overlap, eg byte arrays, you must have different converters
class RowMaterializer(fileSchema: MessageType,
                      readContext: ReadContext) extends RecordMaterializer[Row] {

  val schema = ParquetSchemaFns.fromParquetGroupType(fileSchema)
  val builder = new RowBuilder(schema)
  var row: Row = null

  override def getRootConverter: GroupConverter = new GroupConverter {
    override def getConverter(fieldIndex: Int): Converter = {
      val field = schema.fields(fieldIndex)
      field.dataType match {
        case BinaryType => new DefaultPrimitiveConverter(builder)
        case BooleanType => new DefaultPrimitiveConverter(builder)
        case DateType => new DateConverter(builder)
        case DecimalType(precision, scale) => new DecimalConverter(builder, precision, scale)
        case DoubleType => new DefaultPrimitiveConverter(builder)
        case FloatType => new DefaultPrimitiveConverter(builder)
        case _: IntType => new DefaultPrimitiveConverter(builder)
        case _: LongType => new DefaultPrimitiveConverter(builder)
        case _: ShortType => new DefaultPrimitiveConverter(builder)
        case StringType => new StringPrimitiveConverter(builder)
        case TimestampType => new TimestampConverter(builder)
        case other => sys.error("Unsupported type " + other)
      }
    }
    override def end(): Unit = row = builder.build()
    override def start(): Unit = builder.reset()
  }

  override def getCurrentRecord: Row = row
}

// just adds the parquet type directly into the builder
// for types that are not pass through, create an instance of a more specialized converter
class DefaultPrimitiveConverter(builder: RowBuilder) extends PrimitiveConverter {
  override def addBinary(value: Binary): Unit = builder.add(value.getBytes)
  override def addDouble(value: Double): Unit = builder.add(value)
  override def addLong(value: Long): Unit = builder.add(value)
  override def addBoolean(value: Boolean): Unit = builder.add(value)
  override def addInt(value: Int): Unit = builder.add(value)
  override def addFloat(value: Float): Unit = builder.add(value)
}

class StringPrimitiveConverter(builder: RowBuilder) extends PrimitiveConverter {
  override def addBinary(value: Binary): Unit = builder.add(value.toStringUsingUTF8)
}

// we must use the precision and scale to build the value back from the bytes
class DecimalConverter(builder: RowBuilder,
                       precision: Precision,
                       scale: Scale) extends PrimitiveConverter {
  override def addBinary(value: Binary): Unit = {
    val bi = new BigInteger(value.getBytes)
    val bd = BigDecimal.apply(bi, scale.value, new MathContext(precision.value))
    builder.add(bd)
  }
}

// https://github.com/Parquet/parquet-mr/issues/218
class TimestampConverter(builder: RowBuilder) extends PrimitiveConverter {

  val JulianEpochInGregorian = LocalDateTime.of(-4713, 11, 24, 0, 0, 0)

  override def addBinary(value: Binary): Unit = {
    // first 8 bytes is the nanoseconds
    // second 8 bytes is the day
    val nanos = ByteBuffer.wrap(value.getBytes.slice(0, 8)).order(ByteOrder.LITTLE_ENDIAN).getLong()
    val days = ByteBuffer.wrap(value.getBytes.slice(8, 12)).order(ByteOrder.LITTLE_ENDIAN).getInt()
    println(days)
    val dt = JulianEpochInGregorian.plusDays(days).plusNanos(nanos)
    val millis = dt.atZone(ZoneId.systemDefault).toInstant.toEpochMilli
    builder.add(new Timestamp(millis))
  }
}

class DateConverter(builder: RowBuilder) extends PrimitiveConverter {
  val UnixEpoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0)
  override def addInt(value: Int): Unit = {
    val dt = UnixEpoch.plusDays(value)
    val millis = dt.atZone(ZoneId.systemDefault).toInstant.toEpochMilli
    builder.add(new Date(millis))
  }
}