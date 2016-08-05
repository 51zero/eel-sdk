package io.eels.component.avro

import org.apache.avro.file.{DataFileReader, SeekableFileInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import java.nio.file.Path

object AvroReaderFns {

  def createAvroReader(path: Path): DataFileReader[GenericRecord] = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    new DataFileReader[GenericRecord](new SeekableFileInput(path.toFile()), datumReader)
  }
}