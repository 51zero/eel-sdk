package io.eels.component.avro

import io.eels.util.Logging
import io.eels.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * Marshalls rows as avro records using the given write schema.
 *
 * @recordSchema the schema to be used in the record. Each row must
 * provide all the fields listed in the record schema.
 *
 */
class AvroRecordMarshaller(val recordSchema: Schema) : Logging {

  val fields = recordSchema.fields
  val converters = fields.map { converter(it.schema()) }

  //val converters = fields.map { OptionalConverter(converter(field.schema)) }
  init {
    logger.debug("Avro marshaller created with schema=${fields.map { it.name() }.joinToString (", ")}")
  }

  fun toRecord(row: Row, caseInsensitive: Boolean = false): GenericRecord {
    require(row.size() == recordSchema.fields.size, { "row must provide all fields of the record schema $recordSchema" })
    val record = GenericData.Record(recordSchema)
    for ((field, converter) in fields.zip(converters)) {
      val value = row.get(field.name(), caseInsensitive)
      val converted = if (value == null) null else converter.convert(value)
      record.put(field.name(), converted)
    }
    return record
  }

  private fun converter(schema: Schema): AvroConverter<*> = when (schema.type) {
    Schema.Type.BOOLEAN -> BooleanConverter
    Schema.Type.DOUBLE -> DoubleConverter
    Schema.Type.ENUM -> StringConverter
    Schema.Type.FLOAT -> FloatConverter
    Schema.Type.INT -> IntConverter
    Schema.Type.LONG -> LongConverter
    Schema.Type.STRING -> StringConverter
    else -> {
      logger.warn("No converter exists for fieldType=${schema.type}; defaulting to StringConverter")
      StringConverter
    }
  }

  //  def default(field: AvroSchema.Field) = {
  //    if (field.defaultValue != null) field.defaultValue.getTextValue
  //    else if (fillMissingValues) null
  //    else sys.error(s"Record is missing value for column $field")
  //  }
}