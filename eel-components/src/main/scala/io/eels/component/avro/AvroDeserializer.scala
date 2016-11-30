package io.eels.component.avro

import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.schema.StructType
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.parquet.column.Dictionary

import scala.collection.JavaConverters._

/**
  * Returns an row from the given avro record using the schema present in the record.
  * The row values will be created in the order that the record schema fields are declared.
  */
class AvroDeserializer(useJavaString: Boolean = ConfigFactory.load().getBoolean("eel.avro.java.string")) {

  val config = ConfigFactory.load()
  val deserializeAsNullable = config.getBoolean("eel.avro.deserializeAsNullable")
  var schema: StructType = null
  var fields: Array[Field] = null
  var range: Range = null

  def toRow(record: GenericRecord): Row = {

    // take the schema from the first record
    if (schema == null) {
      schema = AvroSchemaFns.fromAvroSchema(record.getSchema, deserializeAsNullable)
      fields = record.getSchema.getFields.asScala.toArray
      range = fields.indices
    }

    val vector = Vector.newBuilder[Any]
    for (k <- range) {
      val value = record.get(k)
      if (useJavaString && value.isInstanceOf[Utf8]) {
        // use the utf8's toString as it is optimized
        val str = value.asInstanceOf[Utf8].toString
        vector.+=(str)
      } else {
        vector.+=(value)
      }
    }

    Row(schema, vector.result)
  }
}

//   private val config = ConfigFactory.load()
// private val useJavaString = config.getBoolean("eel.avro.java.string")
///**
// * Builds an avro record for the given avro schema, using the given eel schema
// * to determine the correct ordering from the row.
// *
// * The given AcroSchema is for building Record object, as well as for converting types to the
// * right format as expected by the avro writer.
// */
//@deprecated("use the more performant AvroRecordMarshaller", "0.36.0")
//def toRecord(row: InternalRow, avroSchema: AvroSchema, sourceSchema: Schema, config: Config): GenericRecord = {
//
//  def converter(schema: AvroSchema): Converter[_] = {
//    schema.getType match {
//      case AvroSchema.Type.BOOLEAN => BooleanConverter
//          case AvroSchema.Type.DOUBLE => DoubleConverter
//          case AvroSchema.Type.ENUM => StringConverter
//          case AvroSchema.Type.FLOAT => FloatConverter
//          case AvroSchema.Type.INT => IntConverter
//          case AvroSchema.Type.LONG => LongConverter
//          case AvroSchema.Type.STRING => StringConverter
//          case AvroSchema.Type.UNION => converter(schema.getTypes.asScala.find(_.getType != AvroSchema.Type.NULL).get)
//      case other =>
//      logger.warn(s"No converter exists for fieldType=$other; defaulting to StringConverter")
//      StringConverter
//    }
//  }
//
//  val fillMissingValues = config.getBoolean("eel.avro.fillMissingValues")
//
//  def default(field: AvroSchema.Field) = {
//    if (field.defaultValue != null) field.defaultValue.getTextValue
//    else if (fillMissingValues) null
//    else sys.error(s"Record is missing value for column $field")
//  }
//
//  val map = sourceSchema.fieldNames.zip(row).toMap
//  val record = new Record(avroSchema)
//  for (field <- avroSchema.getFields.asScala) {
//    val value = map.getOrElse(field.name, default(field))
//    val converted = new OptionalConverter(converter(field.schema))(value)
//    record.put(field.name, converted)
//  }
//  record
//}