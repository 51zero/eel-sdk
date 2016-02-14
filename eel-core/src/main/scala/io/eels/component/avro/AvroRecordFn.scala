package io.eels.component.avro

import io.eels.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

object AvroRecordFn {

  import scala.collection.JavaConverters._

  def fromRecord(record: GenericRecord): Row = {
    val map = record.getSchema.getFields.asScala.map { field =>
      field.name -> record.get(field.name).toString
    }.toMap
    Row(map)
  }

  def toRecord(row: Row, schema: Schema): GenericRecord = {
    val record = new Record(schema)
    for ( (key, value) <- row.toMap ) {
      record.put(key, value)
    }
    record
  }
}
