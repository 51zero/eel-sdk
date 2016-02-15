package io.eels.component.kafka

import java.io.ByteArrayOutputStream

import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import io.eels.{FrameSchema, Row}
import org.apache.avro.file.{DataFileWriter, DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumWriter, GenericDatumReader, GenericRecord}

object AvroKafkaDeserializer extends KafkaDeserializer {
  override def apply(bytes: Array[Byte]): Row = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    val reader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datumReader)
    val record = reader.next()
    AvroRecordFn.fromRecord(record)
  }
}

object AvroKafkaSerializer extends KafkaSerializer {
  override def apply(row: Row): Array[Byte] = {

    val schema = AvroSchemaGen(FrameSchema(row.columns))
    val record = AvroRecordFn.toRecord(row, schema)

    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    val out = new ByteArrayOutputStream
    dataFileWriter.create(schema, out)
    dataFileWriter.append(record)
    out.toByteArray
  }
}