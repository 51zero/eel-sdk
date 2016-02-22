package io.eels.component.avro

import io.eels.{FrameSchema, Row}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

object AvroRecordFn {

  import scala.collection.JavaConverters._

  def fromRecord(record: GenericRecord): Row = {
    record.getSchema.getFields.asScala.map { field =>
      record.get(field.name)
    }.toVector
  }

  def fromRecord(record: GenericRecord, columns: Seq[String]): Row = {
    columns.map { column =>
      record.get(column)
    }.toVector
  }

  /**
    * Builds an avro record for the given avro schema, using the given frame schema
    * to determine the correct ordering from the row.
    */
  def toRecord(row: Row, avroSchema: Schema, sourceSchema: FrameSchema): GenericRecord = {
    val record = new Record(avroSchema)
    for ((columnName, value) <- sourceSchema.columnNames.zip(row)) {
      record.put(columnName, value)
    }
    record
  }
}
