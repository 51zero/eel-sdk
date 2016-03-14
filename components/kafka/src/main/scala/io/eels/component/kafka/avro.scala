package io.eels.component.kafka

import java.io.ByteArrayOutputStream

import com.typesafe.config.ConfigFactory
import io.eels.{InternalRow, Schema}
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn, ConvertingAvroRecordMarshaller}
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}

object AvroKafkaDeserializer extends KafkaDeserializer {

  val recordFn = new AvroRecordFn

  override def apply(bytes: Array[Byte]): InternalRow = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    val reader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datumReader)
    val record = reader.next()
    recordFn.fromRecord(record)
  }
}

object AvroKafkaSerializer extends KafkaSerializer {

  val config = ConfigFactory.load()

  override def apply(row: InternalRow, schema: Schema): Array[Byte] = {

    val avroSchema = AvroSchemaFn.toAvro(schema)
    val marshaller = new ConvertingAvroRecordMarshaller(avroSchema)

    val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    val out = new ByteArrayOutputStream
    dataFileWriter.create(avroSchema, out)
    dataFileWriter.append(marshaller.toRecord(row))
    out.toByteArray
  }
}