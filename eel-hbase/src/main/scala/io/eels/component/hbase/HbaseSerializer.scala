package io.eels.component.hbase

import java.sql.Timestamp

import io.eels.schema._
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.types.{DataType => _, _}
import org.apache.hadoop.hbase.util.{Bytes, PositionedByteRange, SimplePositionedMutableByteRange}

/**
  * Create an implementation of this interface if you wish to write your own serializer
  */
trait HbaseSerializer {
  def fromBytes(value: Array[Byte], name: String, dataType: DataType): Any

  def toBytes(value: Any, name: String, dataType: DataType): Array[Byte]

}

/**
  * Give me a known serializer
  */
object HbaseSerializer {
  def standardSerializer: HbaseSerializer = new StandardHbaseSerializer

  def orderedAscendingSerializer: HbaseSerializer = new OrderedHbaseSerializerAscending

  def orderedDescendingSerializer: HbaseSerializer = new OrderedHbaseSerializerDescending
}

/**
  * The mostly used mechanism of serializing and de-serializing data for HBase (this is the default)
  *
  * Note: If you plan to use Hive over HBase then ensure your binary numeric types are on of the following types:
  *
  * TINYINT, SMALLINT, BIGINT, FLOAT, DOUBLE
  *
  * The Hive storage handler (org.apache.hadoop.hive.hbase.HbaseStorageHandler) doesn't play too well with other
  * binary numeric types such as BigDecimal and BigInteger - they display incorrectly and yield wrong results when
  * filtering.
  */
class StandardHbaseSerializer extends HbaseSerializer {
  override def fromBytes(value: Array[Byte], name: String, dataType: DataType): Any = dataType match {
    case StringType | VarcharType(_) | CharType(_) => Bytes.toString(value)
    case DoubleType => Bytes.toDouble(value)
    case DecimalType(_, _) => Bytes.toBigDecimal(value)
    case ShortType.Signed => Bytes.toShort(value)
    case IntType.Signed => Bytes.toInt(value)
    case LongType.Signed => Bytes.toLong(value)
    case ByteType.Signed => value.head
    case FloatType => Bytes.toFloat(value)
    case BooleanType => Bytes.toBoolean(value)
    case BinaryType => value
    case TimestampMillisType => new Timestamp(Bytes.toLong(value))
    case _ => sys.error(s"DataType '${dataType.canonicalName}' is not supported for field '$name'")
  }

  override def toBytes(value: Any, name: String, dataType: DataType): Array[Byte] = dataType match {
    case StringType | VarcharType(_) | CharType(_) => Bytes.toBytes(HbaseCoercers.StringCoercer.coerce(value))
    case DoubleType => Bytes.toBytes(HbaseCoercers.DoubleCoercer.coerce(value))
    case DecimalType(_, _) => Bytes.toBytes(HbaseCoercers.DecimalCoercer.coerce(value))
    case ShortType.Signed => Bytes.toBytes(HbaseCoercers.ShortCoercer.coerce(value))
    case IntType.Signed => Bytes.toBytes(HbaseCoercers.IntCoercer.coerce(value))
    case LongType.Signed => Bytes.toBytes(HbaseCoercers.LongCoercer.coerce(value))
    case ByteType.Signed => Bytes.toBytes(HbaseCoercers.ByteCoercer.coerce(value))
    case FloatType => Bytes.toBytes(HbaseCoercers.FloatCoercer.coerce(value))
    case BooleanType => Bytes.toBytes(HbaseCoercers.BooleanCoercer.coerce(value))
    case BinaryType => value.asInstanceOf[Array[Byte]]
    case TimestampMillisType => Bytes.toBytes(HbaseCoercers.TimestampCoercer.coerce(value).getTime)
    case _ => sys.error(s"DataType '${dataType.canonicalName}' is not supported for field '$name'")
  }
}


abstract class OrderingHbaseSerializerBase(ascending: Boolean) extends HbaseSerializer {

  private val StringSerDe = if (ascending) OrderedString.ASCENDING else OrderedString.DESCENDING
  private val BinarySerDe = if (ascending) OrderedBlob.ASCENDING else OrderedBlob.DESCENDING
  private val DoubleSerDe = if (ascending) OrderedFloat64.ASCENDING else OrderedFloat64.DESCENDING
  private val NumericSerDe = if (ascending) OrderedNumeric.ASCENDING else OrderedNumeric.DESCENDING
  private val IntSerDe = if (ascending) OrderedInt32.ASCENDING else OrderedInt32.DESCENDING
  private val LongSerDe = if (ascending) OrderedInt64.ASCENDING else OrderedInt64.DESCENDING
  private val FloatSerDe = if (ascending) OrderedFloat32.ASCENDING else OrderedFloat32.DESCENDING
  private val ShortSerDe = if (ascending) OrderedInt16.ASCENDING else OrderedInt16.DESCENDING
  private val TimestampSerDe = if (ascending) OrderedInt64.ASCENDING else OrderedInt64.DESCENDING
  private val BooleanSerDe = if (ascending) OrderedInt8.ASCENDING else OrderedInt8.DESCENDING
  private val ByteSerDe = if (ascending) OrderedInt8.ASCENDING else OrderedInt8.DESCENDING

  private val threadLocalBuffer = new ThreadLocal[PositionedByteRange]() {
    override def initialValue(): PositionedByteRange = {
      new SimplePositionedMutableByteRange().set(HConstants.MAX_ROW_LENGTH)
    }
  }

  override def fromBytes(value: Array[Byte], name: String, dataType: DataType): Any = dataType match {
    case StringType | VarcharType(_) | CharType(_) => StringSerDe.decode(populateBuffer(value))
    case DoubleType => DoubleSerDe.decode(populateBuffer(value))
    case DecimalType(_, _) => NumericSerDe.decode(populateBuffer(value))
    case ShortType.Signed => ShortSerDe.decode(populateBuffer(value))
    case IntType.Signed => IntSerDe.decode(populateBuffer(value))
    case LongType.Signed => LongSerDe.decode(populateBuffer(value))
    case ByteType.Signed => ByteSerDe.decode(populateBuffer(value))
    case FloatType => FloatSerDe.decode(populateBuffer(value))
    case BooleanType => BooleanSerDe.decode(populateBuffer(value))
    case BinaryType => BinarySerDe.decode(populateBuffer(value))
    case TimestampMillisType => TimestampSerDe.decode(populateBuffer(value))
    case _ => sys.error(s"DataType '${dataType.canonicalName}' is not supported for field '$name'")
  }

  override def toBytes(value: Any, name: String, dataType: DataType): Array[Byte] = {
    val buffer = newBuffer
    dataType match {
      case StringType | VarcharType(_) | CharType(_) => copy(buffer, StringSerDe.encode(buffer, HbaseCoercers.StringCoercer.coerce(value)))
      case DoubleType => copy(buffer, DoubleSerDe.encode(buffer, HbaseCoercers.DoubleCoercer.coerce(value)))
      case DecimalType(_, _) => copy(buffer, NumericSerDe.encode(buffer, HbaseCoercers.DecimalCoercer.coerce(value)))
      case ShortType.Signed => copy(buffer, ShortSerDe.encode(buffer, HbaseCoercers.ShortCoercer.coerce(value)))
      case IntType.Signed => copy(buffer, IntSerDe.encode(buffer, HbaseCoercers.IntCoercer.coerce(value)))
      case LongType.Signed => copy(buffer, LongSerDe.encode(buffer, HbaseCoercers.LongCoercer.coerce(value)))
      case ByteType.Signed => copy(buffer, ByteSerDe.encode(buffer, HbaseCoercers.ByteCoercer.coerce(value)))
      case FloatType => copy(buffer, FloatSerDe.encode(buffer, HbaseCoercers.FloatCoercer.coerce(value)))
      case BooleanType => copy(buffer, BooleanSerDe.encode(buffer, HbaseCoercers.BooleanCoercer.coerce(value).toString.toByte))
      case BinaryType => copy(buffer, BinarySerDe.encode(buffer, HbaseCoercers.BinaryCoercer.coerce(value)))
      case TimestampMillisType => copy(buffer, TimestampSerDe.encode(buffer, HbaseCoercers.TimestampCoercer.coerce(value).getTime))
      case _ => sys.error(s"DataType '${dataType.canonicalName}' is not supported for field '$name'")
    }
  }

  private def populateBuffer(value: Byte): PositionedByteRange = populateBuffer(Array(value))

  private def populateBuffer(value: Array[Byte]): PositionedByteRange = {
    val byteRange = threadLocalBuffer.get()
    byteRange.setOffset(0)
    if (value != null) byteRange.set(value)
    byteRange
  }

  private def newBuffer: PositionedByteRange = threadLocalBuffer.get().setOffset(0)

  private def copy(source: PositionedByteRange, len: Int): Array[Byte] = {
    val bufferCopy = new Array[Byte](len)
    System.arraycopy(source.getBytes, 0, bufferCopy, 0, bufferCopy.length)
    bufferCopy
  }
}

class OrderedHbaseSerializerAscending extends OrderingHbaseSerializerBase(true)

class OrderedHbaseSerializerDescending extends OrderingHbaseSerializerBase(false)
