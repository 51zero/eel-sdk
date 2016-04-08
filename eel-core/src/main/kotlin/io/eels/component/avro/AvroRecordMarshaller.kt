package io.eels.component.avro

import io.eels.Logging
import io.eels.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * Converts eel rows into avro records using the given avro schema.
 * Each row must provide a value for each field in the schema, and the the order of the values
 * in the row is assumed to be the same order as the order of the fields in the schema.
 * Each row value will be converted into the appropriate type for the field.
 */
class AvroRecordMarshaller(val schema: Schema) : Logging {

  val fields = schema.fields

  //val converters = fields.map { OptionalConverter(converter(field.schema)) }
  init {
    logger.debug("Avro marshaller created with schema=${fields.map { it.name() }.joinToString (", ")}")
  }

  fun toRecord(row: Row): GenericRecord {
    require(
        row.size() == schema.fields.size,
        {
          """AvroRecordMarshaller cannot marshall; size of row and size of schema differ;schema=${fields.map { it.name() }.joinToString (", ")};values=$row"""
        }
    )
    val record = GenericData.Record(schema)
    for (k in 0 until row.size()) {
      record.put(schema.fields.get(k).name(), row.get(k))
    }
    return record
  }

  //  private fun converter(schema: Schema): Converter[_]   {
  //    schema.getType match {
  //      case Schema.Type.BOOLEAN => BooleanConverter
  //          case Schema.Type.DOUBLE => DoubleConverter
  //          case Schema.Type.ENUM => StringConverter
  //          case Schema.Type.FLOAT => FloatConverter
  //          case Schema.Type.INT => IntConverter
  //          case Schema.Type.LONG => LongConverter
  //          case Schema.Type.STRING => StringConverter
  //          case Schema.Type.UNION => converter(schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get)
  //      case other =>
  //      logger.warn(s"No converter exists for fieldType=$other; defaulting to StringConverter")
  //      StringConverter
  //    }

  //  def default(field: AvroSchema.Field) = {
  //    if (field.defaultValue != null) field.defaultValue.getTextValue
  //    else if (fillMissingValues) null
  //    else sys.error(s"Record is missing value for column $field")
  //  }
}