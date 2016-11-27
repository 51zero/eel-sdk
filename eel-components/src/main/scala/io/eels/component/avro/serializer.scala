package io.eels.component.avro

import com.sksamuel.exts.Logging
import io.eels.Row
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.JavaConverters._

/**
  * Serializes JVM types into the suitable Avro type.
  *
  * Rows are serialized into records
  * Scala collections are serialized into arrays
  * Options into nulls.
  *
  * Also, if you have an Avro Boolean field, you can't use a String "true" but it must be boolean true.
  * So it's less "forgiving" than JDBC for example which will do the automatic conversion for you.
  * This serializer allows it.
  *
  */
trait AvroSerializer {
  def serialize(value: Any): Any
}

object AvroSerializer extends Logging {
  def apply(schema: Schema): AvroSerializer = {
    schema.getType match {
      case Schema.Type.ARRAY => new ArraySerializer(AvroSerializer(schema.getElementType))
      case Schema.Type.BOOLEAN => BooleanSerializer
      case Schema.Type.BYTES => BytesSerializer
      case Schema.Type.DOUBLE => DoubleSerializer
      case Schema.Type.ENUM => StringSerializer
      case Schema.Type.FIXED => BytesSerializer
      case Schema.Type.FLOAT => FloatSerializer
      case Schema.Type.INT => IntSerializer
      case Schema.Type.LONG => LongSerializer
      case Schema.Type.MAP => MapSerializer
      case Schema.Type.RECORD => new RecordSerializer(schema)
      case Schema.Type.STRING => StringSerializer
      case Schema.Type.UNION =>
        val nonNullType = schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).getOrElse(sys.error("Bug"))
        new OptionSerializer(AvroSerializer(nonNullType))
      case _ =>
        sys.error(s"No avro serializer exists for schema=$schema")
        StringSerializer
    }
  }
}

/**
  * Marshalls rows as avro records using the given schema.
  *
  * @param schema the schema to be used in the record. Each input value must
  *               provide all the fields listed in the schema.
  */
class RecordSerializer(schema: Schema) extends AvroSerializer {

  private val avroFields: Seq[Schema.Field] = schema.getFields.asScala
  private val serializers = avroFields.map { it => AvroSerializer(it.schema) }

  private def explode(value: Any): Seq[Any] = value match {
    // row must be first as it IS a product also
    case row: Row =>
      require(row.size() == schema.getFields.size, s"row must match the target schema $schema")
      row.values
    // must force a non stream
    case product: Product => product.productIterator.toList
    case iter: Iterator[_] => iter.toList
  }

  override def serialize(value: Any): GenericRecord = {
    val record = new GenericData.Record(schema)
    explode(value).zip(serializers).zip(avroFields).foreach { case ((x, serializer), field) =>
      val converted = if (x == null) null else serializer.serialize(x)
      record.put(field.name(), converted)
    }
    record
  }
}

// wraps another serializer extracting somes and turning nones into nulls
class OptionSerializer(serializer: AvroSerializer) extends AvroSerializer {
  override def serialize(value: Any): Any = value match {
    case Some(x) => serializer.serialize(x)
    case None => null
    case other => serializer.serialize(other)
  }
}

object MapSerializer extends AvroSerializer {
  override def serialize(value: Any): Map[_, _] = value match {
    case map: Map[_, _] => map
    case map: java.util.Map[_, _] => map.asScala.toMap
  }
}

object BytesSerializer extends AvroSerializer {
  override def serialize(value: Any): Array[Byte] = value match {
    case str: String => str.getBytes
    case bytes: Array[Byte] => bytes
  }
}

class ArraySerializer(serializer: AvroSerializer) extends AvroSerializer {
  override def serialize(value: Any): Any = value match {
    case seq: Iterable[_] => seq.map(serializer.serialize).toArray[Any]
    case list: List[_] => list.map(serializer.serialize).toArray[Any]
    case col: java.lang.Iterable[_] => serialize(col.asScala)
  }
}

object StringSerializer extends AvroSerializer {
  override def serialize(value: Any): String = value.toString()
}

object BooleanSerializer extends AvroSerializer {
  override def serialize(value: Any): Boolean = value match {
    case b: Boolean => b
    case "true" => true
    case "false" => false
    case _ => sys.error("Could not convert $value to Boolean")
  }
}

object IntSerializer extends AvroSerializer {
  override def serialize(value: Any): Int = value match {
    case i: Int => i
    case l: Long => l.toInt
    case _ => value.toString().toInt
  }
}

object ShortSerializer extends AvroSerializer {
  override def serialize(value: Any): Short = value match {
    case s: Short => s
    case i: Int => i.toShort
    case l: Long => l.toShort
    case _ => value.toString().toShort
  }
}

object LongSerializer extends AvroSerializer {
  override def serialize(value: Any): Long = value match {
    case l: Long => l
    case s: Short => s.toLong
    case i: Int => i.toLong
    case _ => value.toString().toLong
  }
}

object DoubleSerializer extends AvroSerializer {
  override def serialize(value: Any): Double = value match {
    case d: Double => d
    case f: Float => f.toDouble
    case l: Long => l.toDouble
    case i: Int => i.toDouble
    case _ => value.toString().toDouble
  }
}

object FloatSerializer extends AvroSerializer {
  override def serialize(value: Any): Float = value match {
    case f: Float => f
    case d: Double => d.toFloat
    case l: Long => l.toFloat
    case i: Int => i.toFloat
    case _ => value.toString().toFloat
  }
}