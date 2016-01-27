package com.sksamuel.eel.sink

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.sksamuel.eel.{Row, Sink, Writer}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

import scala.collection.JavaConverters._

case class AvroSink(out: OutputStream) extends Sink {

  def writer: Writer = new Writer {

    var writer: DataFileWriter[GenericRecord] = null

    override def write(row: Row): Unit = {
      if (writer == null)
        writer = createWriter(row)
      writer.append(toRecord(row))
    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
    }

    private def createWriter(row: Row): DataFileWriter[GenericRecord] = {
      val datumWriter = new GenericDatumWriter[GenericRecord](schema(row))
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema(row), out)
      dataFileWriter
    }

    private def schema(row: Row): Schema = {
      val schema = Schema.createRecord("row", "", "packge", false)
      val fields = row.columns.map(col => new Schema.Field(col.name, Schema.create(Schema.Type.STRING), "", null))
      schema.setFields(fields.asJava)
      schema
    }

    private def toRecord(row: Row): GenericRecord = {
      val record = new Record(schema(row))
      row.toMap.foreach { case (key, value) => record.put(key, value) }
      record
    }
  }
}

object AvroSink {
  def apply(path: Path): AvroSink = AvroSink(Files.newOutputStream(path))
  def apply(file: File): AvroSink = apply(file.toPath)
}