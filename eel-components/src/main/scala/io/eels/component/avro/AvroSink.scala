package io.eels.component.avro

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}

case class AvroSink(out: OutputStream) extends Sink {
  override def writer(schema: StructType): SinkWriter = new SinkWriter {
    private val writer = new AvroWriter(schema, out)
    override def write(row: Row): Unit = writer.write(row)
    override def close(): Unit = writer.close()
  }
}

object AvroSink {
  def apply(path: Path): AvroSink = AvroSink(Files.newOutputStream(path))
  def apply(file: File): AvroSink = apply(file.toPath)
}