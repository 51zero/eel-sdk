package io.eels.component.avro

import com.sksamuel.scalax.Logging
import io.eels.Converter._
import io.eels.{Converter, InternalRow}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

/**
  * Converts eel rows into avro records using the given avro schema.
  * Each row must provide a value for each field in the schema, and the the order of the values
  * in the row is assumed to be the same order as the order of the fields in the schema.
  * Each row value will be converted into the appropriate type for the field.
  */
class ConvertingAvroRecordMarshaller(schema: Schema) extends AvroRecordMarshaller with Logging {

  private val fields = schema.getFields.asScala.toArray
  private val converters = fields.map { field => new OptionalConverter(converter(field.schema)) }
  logger.debug(s"Avro marshaller created with schema=${fields.map(_.name).mkString(", ")}")

  override def toRecord(row: InternalRow): GenericRecord = {
    require(row.size == fields.length,
      s"""AvroRecordMarshaller cannot marshall a row which does not have the same number of fields as the supplied avro schema
          |SchemaFields=${fields.map(_.name).mkString(", ")}
          |RowValues=$row""".stripMargin
    )
    val record = new Record(schema)
    for (k <- row.indices) {
      val value = row(k)
      val converted = converters(k)(value)
      record.put(fields(k).name, converted)
    }
    record
  }

  private def converter(schema: Schema): Converter[_] = {
    schema.getType match {
      case Schema.Type.BOOLEAN => BooleanConverter
      case Schema.Type.DOUBLE => DoubleConverter
      case Schema.Type.ENUM => StringConverter
      case Schema.Type.FLOAT => FloatConverter
      case Schema.Type.INT => IntConverter
      case Schema.Type.LONG => LongConverter
      case Schema.Type.STRING => StringConverter
      case Schema.Type.UNION => converter(schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get)
      case other =>
        logger.warn(s"No converter exists for fieldType=$other; defaulting to StringConverter")
        StringConverter
    }
  }

  //  def default(field: AvroSchema.Field) = {
  //    if (field.defaultValue != null) field.defaultValue.getTextValue
  //    else if (fillMissingValues) null
  //    else sys.error(s"Record is missing value for column $field")
  //  }
}