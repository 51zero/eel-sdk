package io.eels.component.avro

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.typesafe.config.{Config, ConfigFactory}
import io.eels.{InternalRow, Schema, Sink, SinkWriter}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic
import org.apache.avro.generic.GenericRecord

case class AvroSink(out: OutputStream) extends Sink {
  private val config = ConfigFactory.load()
  override def writer(schema: Schema): SinkWriter = new AvroSinkWriter(schema, out, config)
}

class AvroSinkWriter(schema: Schema, out: OutputStream, config: Config) extends SinkWriter {

  val avroSchema = AvroSchemaFn.toAvro(schema)
  val datumWriter = new generic.GenericDatumWriter[GenericRecord](avroSchema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(avroSchema, out)

  override def write(row: InternalRow): Unit = {
    val record = AvroRecordFn.toRecord(row, avroSchema, schema, config)
    dataFileWriter.append(record)
  }

  override def close(): Unit = {
    dataFileWriter.flush()
    dataFileWriter.close()
  }
}

object AvroSink {
  def apply(path: Path): AvroSink = AvroSink(Files.newOutputStream(path))
  def apply(file: File): AvroSink = apply(file.toPath)
}