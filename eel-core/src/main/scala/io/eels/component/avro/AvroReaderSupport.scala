package io.eels.component.avro

import java.nio.file.Path

import org.apache.avro.file.{DataFileReader, SeekableFileInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

object AvroReaderSupport {

  def createReader(path: Path): DataFileReader[GenericRecord] = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    new DataFileReader[GenericRecord](new SeekableFileInput(path.toFile), datumReader)
  }
}
