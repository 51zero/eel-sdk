package com.sksamuel.eel.source

import java.nio.file.Path

import com.sksamuel.eel.{Reader, Row, Source}
import org.apache.avro.file.{DataFileReader, SeekableFileInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConverters._

case class AvroSource(path: Path) extends Source {

  override def reader: Reader = new Reader {

    private val datumReader = new GenericDatumReader[GenericRecord]()
    private val dataFileReader = new DataFileReader[GenericRecord](new SeekableFileInput(path.toFile), datumReader)

    override def close(): Unit = dataFileReader.close()

    override val iterator: Iterator[Row] = new Iterator[Row] {

      override def hasNext: Boolean = {
        val hasNext = dataFileReader.hasNext
        if (!hasNext)
          close()
        hasNext
      }

      override def next: Row = {
        val record = dataFileReader.next
        val map = record.getSchema.getFields.asScala.map { field =>
          field.name -> record.get(field.name).toString
        }.toMap
        Row(map)
      }
    }
  }
}
