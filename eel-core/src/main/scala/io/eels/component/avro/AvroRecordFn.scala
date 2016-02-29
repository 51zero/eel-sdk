package io.eels.component.avro

import com.sksamuel.scalax.Logging
import com.typesafe.config.Config
import io.eels.Converter._
import io.eels.{Converter, InternalRow, Schema}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema => AvroSchema}

import scala.collection.JavaConverters._

object AvroRecordFn extends Logging {

  def fromRecord(record: GenericRecord): InternalRow = {
    record.getSchema.getFields.asScala.map { field =>
      record.get(field.name)
    }.toVector
  }

  def fromRecord(record: GenericRecord, columns: Seq[String]): InternalRow = {
    columns.map { column =>
      record.get(column)
    }.toVector
  }

  /**
    * Builds an avro record for the given avro schema, using the given eel schema
    * to determine the correct ordering from the row.
    *
    * The given AcroSchema is for building Record object, as well as for converting types to the
    * right format as expected by the avro writer.
    */
  def toRecord(row: InternalRow, avroSchema: AvroSchema, sourceSchema: Schema, config: Config): GenericRecord = {

    def converter(schema: AvroSchema): Converter[_] = {
      schema.getType match {
        case AvroSchema.Type.BOOLEAN => BooleanConverter
        case AvroSchema.Type.DOUBLE => DoubleConverter
        case AvroSchema.Type.ENUM => StringConverter
        case AvroSchema.Type.FLOAT => FloatConverter
        case AvroSchema.Type.INT => IntConverter
        case AvroSchema.Type.LONG => LongConverter
        case AvroSchema.Type.STRING => StringConverter
        case AvroSchema.Type.UNION => converter(schema.getTypes.asScala.find(_.getType != AvroSchema.Type.NULL).get)
        case other =>
          logger.warn(s"No converter exists for fieldType=$other; defaulting to StringConverter")
          StringConverter
      }
    }

    val fillMissingValues = config.getBoolean("eel.avro.fillMissingValues")

    def default(field: AvroSchema.Field) = {
      if (field.defaultValue != null) field.defaultValue.getTextValue
      else if (fillMissingValues) null
      else sys.error(s"Record is missing value for column $field")
    }

    val map = sourceSchema.columnNames.zip(row).toMap
    val record = new Record(avroSchema)
    for (field <- avroSchema.getFields.asScala) {
      val value = map.getOrElse(field.name, default(field))
      val converted = new OptionalConverter(converter(field.schema))(value)
      record.put(field.name, converted)
    }
    record
  }
}
