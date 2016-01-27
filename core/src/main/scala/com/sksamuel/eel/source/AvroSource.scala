package com.sksamuel.eel.source

import java.nio.file.Path

import com.sksamuel.eel.{Row, Source}
import org.apache.avro.file.{DataFileReader, SeekableFileInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConverters._

case class AvroSource(path: Path) extends Source {

  private def iterator: Iterator[Row] = new Iterator[Row] {

    val datumReader = new GenericDatumReader[GenericRecord]()
    val dataFileReader = new DataFileReader[GenericRecord](new SeekableFileInput(path.toFile), datumReader)

    override def hasNext: Boolean = dataFileReader.hasNext

    override def next(): Row = {
      val record = dataFileReader.next
      val map = record.getSchema.getFields.asScala.map { field =>
        field.name -> record.get(field.name).toString
      }.toMap
      Row(map)
    }
  }

  override def loader: Iterator[Row] = iterator
}
