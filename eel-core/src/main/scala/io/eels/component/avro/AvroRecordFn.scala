package io.eels.component.avro

import com.sksamuel.scalax.Logging
import com.typesafe.config.ConfigFactory
import io.eels.{FrameSchema, InternalRow}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

object AvroRecordFn extends Logging {

  val config = ConfigFactory.load()
  val replaceMissing = config.getBoolean("eel.avro.fillMissingValues")
  logger.debug(s"Avro records replaceMissing=$replaceMissing")

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
    * Builds an avro record for the given avro schema, using the given frame schema
    * to determine the correct ordering from the row.
    */
  def toRecord(row: InternalRow, avroSchema: Schema, sourceSchema: FrameSchema): GenericRecord = {

    def default(field: Schema.Field) = {
      if (replaceMissing) null
      else sys.error(s"Record is missing value for column $field")
    }

    val map = sourceSchema.columnNames.zip(row).toMap
    val record = new Record(avroSchema)
    for (field <- avroSchema.getFields.asScala) {
      val value = map.getOrElse(field.name, default(field))
      record.put(field.name, value)
    }
    record
  }
}
