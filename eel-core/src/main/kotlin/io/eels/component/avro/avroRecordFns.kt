package io.eels.component.avro

import com.typesafe.config.ConfigFactory
import io.eels.Row
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

private val config = ConfigFactory.load()
private val useJavaString = config.getBoolean("eel.avro.java.string")

/**
 * Returns an Eel Row from the given record using the schema present in the record.
 */
fun fromRecord(record: GenericRecord): Row {
  val eelSchema = fromAvro(record.schema)
  val values = record.schema.fields.map { field ->
    val value = record.get(field.name())
    if (useJavaString) {
      when (value) {
        is Utf8 -> String(value.bytes)
        else -> value
      }
    } else {
      value
    }
  }
  return Row(eelSchema, values)
}