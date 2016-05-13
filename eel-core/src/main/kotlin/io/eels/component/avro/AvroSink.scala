package io.eels.component.avro

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.typesafe.config.{Config, ConfigFactory}
import io.eels.{Schema, Sink, SinkWriter}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic
import org.apache.avro.generic.GenericRecord

case class AvroSink(out: OutputStream) extends Sink {
  private val config = ConfigFactory.load()
  override def writer(schema: Schema): SinkWriter = new AvroSinkWriter(schema, out, config)
}

class AvroSinkWriter(schema: Schema, out: OutputStream, config: Config) extends SinkWriter {

  private val caseSensitive = config.getBoolean("eel.avro.caseSensitive")

  val avroSchema = AvroSchemaFn.toAvro(schema, caseSensitive = caseSensitive)
  val datumWriter = new generic.GenericDatumWriter[GenericRecord](avroSchema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(avroSchema, out)

  private val marshaller = new ConvertingAvroRecordMarshaller(avroSchema)

  override def write(row: InternalRow): Unit = {
    val record = marshaller.toRecord(row)
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