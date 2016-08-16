package io.eels.component.avro

import com.sksamuel.exts.Logging
import io.eels.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import scala.collection.JavaConverters._

/**
  * Marshalls rows as avro records using the given write schema.
  *
  * @param recordSchema the schema to be used in the record. Each row must
  *                     provide all the fields listed in the record schema.
  */
class AvroRecordSerializer(val recordSchema: Schema) extends Logging {

  private val fields: List[Schema.Field] = recordSchema.getFields.asScala.toList

  // todo converters should be cached
  private val converters = fields.map { field => converter(field.schema()) }

  //val converters = fields.map { OptionalConverter(converter(field.schema)) }
  logger.debug(s"Avro marshaller created with schema=${fields.map(_.name).mkString(", ")}")

  def toRecord(row: Row, caseInsensitive: Boolean = false): GenericRecord = {
    require(row.size() == recordSchema.getFields.size, s"row must provide all fields of the record schema $recordSchema")
    val record = new GenericData.Record(recordSchema)
    for ((field, converter) <- fields.zip(converters)) {
      val value = row.get(field.name(), caseInsensitive)
      val converted = if (value == null) null else converter.convert(value)
      record.put(field.name(), converted)
    }
    record
  }

  private def converter(schema: Schema): AvroConverter[_] = schema.getType match {
    case Schema.Type.BOOLEAN => BooleanConverter
    case Schema.Type.DOUBLE => DoubleConverter
    case Schema.Type.ENUM => StringConverter
    case Schema.Type.FLOAT => FloatConverter
    case Schema.Type.INT => IntConverter
    case Schema.Type.LONG => LongConverter
    case Schema.Type.STRING => StringConverter
    case Schema.Type.UNION =>
      val nonNullType = schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).getOrElse(sys.error("Bug"))
      new NullableConverter(converter(nonNullType))
    case _ =>
      logger.warn(s"No converter exists for schema=$schema; defaulting to StringConverter")
      StringConverter
  }

  //  def default(field: AvroSchema.Field) = {
  //    if (field.defaultValue != null) field.defaultValue.getTextValue
  //    else if (fillMissingValues) null
  //    else sys.error(s"Record is missing value for column $field")
  //  }
}