package io.eels.component.avro

import io.eels.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

object AvroRecordFn {

  import scala.collection.JavaConverters._

  def fromRecord(record: GenericRecord): Row = {
    record.getSchema.getFields.asScala.map { field =>
      record.get(field.name)
    }
  }

  def toRecord(row: Row, schema: Schema): GenericRecord = {
    val record = new Record(schema)
    for ( (field, value) <- schema.getFields.asScala.zip(row) ) {
      record.put(field.name, value)
    }
    record
  }
}
