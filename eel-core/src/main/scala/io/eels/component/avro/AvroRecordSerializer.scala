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
  * @param writeSchema the schema to be used in the record. Each row must
  *                    provide all the fields listed in the record schema.
  */
class AvroRecordSerializer(val writeSchema: Schema) extends Logging {

  private val fields: List[Schema.Field] = writeSchema.getFields.asScala.toList

  private val converters = fields.map { field => AvroConverter(field.schema()) }

  //val converters = fields.map { OptionalConverter(converter(field.schema)) }

  def toRecord(row: Row, caseInsensitive: Boolean = false): GenericRecord = {
    require(row.size() == writeSchema.getFields.size, s"row must match the target schema $writeSchema")
    val record = new GenericData.Record(writeSchema)
    for ((field, converter) <- fields.zip(converters)) {
      val value = row.get(field.name(), caseInsensitive)
      val converted = if (value == null) null else converter.convert(value)
      record.put(field.name(), converted)
    }
    record
  }

  //  def default(field: AvroSchema.Field) = {
  //    if (field.defaultValue != null) field.defaultValue.getTextValue
  //    else if (fillMissingValues) null
  //    else sys.error(s"Record is missing value for column $field")
  //  }
}