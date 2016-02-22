package io.eels.component.kafka

import java.io.ByteArrayOutputStream

import io.eels.{InternalRow, FrameSchema}
import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}

object AvroKafkaDeserializer extends KafkaDeserializer {
  override def apply(bytes: Array[Byte]): InternalRow = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    val reader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datumReader)
    val record = reader.next()
    AvroRecordFn.fromRecord(record)
  }
}

object AvroKafkaSerializer extends KafkaSerializer {
  override def apply(row: InternalRow, schema: FrameSchema): Array[Byte] = {

    val avroSchema = AvroSchemaGen(schema)
    val record = AvroRecordFn.toRecord(row, avroSchema, schema)

    val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    val out = new ByteArrayOutputStream
    dataFileWriter.create(avroSchema, out)
    dataFileWriter.append(record)
    out.toByteArray
  }
}