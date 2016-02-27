package io.eels.component.avro

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.typesafe.config.ConfigFactory
import io.eels.{Schema, InternalRow, Sink, Writer}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic
import org.apache.avro.generic.GenericRecord

case class AvroSink(out: OutputStream) extends Sink {

  val config = ConfigFactory.load()

  def writer: Writer = new Writer {

    var writer: DataFileWriter[GenericRecord] = null

    override def write(row: InternalRow, schema: Schema): Unit = {
      if (writer == null)
        writer = createWriter(row, schema)
      val avroSchema = AvroSchemaFn.toAvro(schema)
      val record = AvroRecordFn.toRecord(row, avroSchema, schema, config)
      writer.append(record)
    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
    }

    private def createWriter(row: InternalRow, schema: Schema): DataFileWriter[GenericRecord] = {
      val avroSchema = AvroSchemaFn.toAvro(schema)
      val datumWriter = new generic.GenericDatumWriter[GenericRecord](avroSchema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(avroSchema, out)
      dataFileWriter
    }
  }
}

object AvroSink {
  def apply(path: Path): AvroSink = AvroSink(Files.newOutputStream(path))
  def apply(file: File): AvroSink = apply(file.toPath)
}