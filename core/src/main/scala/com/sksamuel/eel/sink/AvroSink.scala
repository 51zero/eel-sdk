package com.sksamuel.eel.sink

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.sksamuel.eel.Sink
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}

import scala.collection.JavaConverters._

case class AvroSink(out: OutputStream) extends Sink {

  var writer: DataFileWriter[GenericRecord] = null

  private def schema(row: Row): Schema = {
    val schema = Schema.createRecord("row", "", "packge", false)
    val fields = row.columns.map(col => new Schema.Field(col.name, Schema.create(Schema.Type.STRING), "", null))
    schema.setFields(fields.asJava)
    schema
  }

  private def createWriter(row: Row): DataFileWriter[GenericRecord] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema(row))
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema(row), out)
    dataFileWriter
  }

  override def completed(): Unit = {
    writer.flush()
    writer.close()
  }

  override def insert(row: Row): Unit = {
    if (writer == null)
      writer = createWriter(row)
    val record = new GenericData.Record(schema(row))
    row.toMap.foreach { case (key, value) => record.put(key, value) }
    writer.append(record)
  }
}

object AvroSink {
  def apply(path: Path): AvroSink = AvroSink(Files.newOutputStream(path))
  def apply(file: File): AvroSink = apply(file.toPath)
}