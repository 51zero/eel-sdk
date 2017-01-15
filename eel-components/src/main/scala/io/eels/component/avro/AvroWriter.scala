package io.eels.component.avro

import java.io.OutputStream

import io.eels.Row
import io.eels.schema.StructType
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic
import org.apache.avro.generic.GenericRecord

class AvroWriter(structType: StructType, out: OutputStream) {

  private val schema = AvroSchemaFns.toAvroSchema(structType)
  private val datumWriter = new generic.GenericDatumWriter[GenericRecord](schema)
  private val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  private val serializer = new RecordSerializer(schema)

  dataFileWriter.create(schema, out)

  def write(row: Row): Unit = {
    val record = serializer.serialize(row)
    dataFileWriter.append(record)
  }

  def close(): Unit = {
    dataFileWriter.flush()
    dataFileWriter.close()
  }
}
