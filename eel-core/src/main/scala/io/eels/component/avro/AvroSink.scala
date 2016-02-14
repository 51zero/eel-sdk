package io.eels.component.avro

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

case class AvroSink(out: OutputStream) extends Sink {

  def writer: Writer = new Writer {

    var writer: DataFileWriter[GenericRecord] = null

    override def write(row: Row): Unit = {
      if (writer == null)
        writer = createWriter(row)
      val schema = AvroSchemaGen(FrameSchema(row.columns))
      val record = AvroRecordFn.toRecord(row, schema)
      writer.append(record)
    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
    }

    private def createWriter(row: Row): DataFileWriter[GenericRecord] = {
      val datumWriter = new GenericDatumWriter[GenericRecord](AvroSchemaGen(FrameSchema(row.columns)))
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(AvroSchemaGen(FrameSchema(row.columns)), out)
      dataFileWriter
    }
  }
}

object AvroSink {
  def apply(path: Path): AvroSink = AvroSink(Files.newOutputStream(path))
  def apply(file: File): AvroSink = apply(file.toPath)
}