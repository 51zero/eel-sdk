package io.eels.component.avro

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path, StandardOpenOption}

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

  def apply(path: Path): AvroSink = apply(path, false)

  def apply(path: Path, overwrite: Boolean): AvroSink = {
    if (path.toFile.exists)
      path.toFile.delete()
    val options = if (overwrite) {
      Seq(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      Seq(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }
    AvroSink(Files.newOutputStream(path, options: _*))
  }

  def apply(file: File): AvroSink = apply(file, false)
  def apply(file: File, overwrite: Boolean): AvroSink = apply(file.toPath, overwrite)
}