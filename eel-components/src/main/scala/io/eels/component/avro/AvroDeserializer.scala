package io.eels.component.avro

import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.schema.StructType
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

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

  def toScala(value: Any): Any = {
    value match {
      case record: GenericRecord => toValues(record)
      case utf8: Utf8 if useJavaString => value.asInstanceOf[Utf8].toString
      case col: java.util.Collection[Any] => col.asScala.toVector.map(toScala)
      case other => other
    }
  }

  def toValues(record: GenericRecord): Vector[Any] = {
    val vector = Vector.newBuilder[Any]
    for (k <- 0 until record.getSchema.getFields.size) {
      val value = record.get(k)
      vector += toScala(value)
    }
    vector.result
  }

  def toRow(record: GenericRecord): Row = {
    // take the schema from the first record
    if (schema == null) {
      schema = AvroSchemaFns.fromAvroSchema(record.getSchema, deserializeAsNullable)
    }
    Row(schema, toValues(record))
  }
}