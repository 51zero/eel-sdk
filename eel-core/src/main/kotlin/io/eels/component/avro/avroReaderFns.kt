package io.eels.component.avro

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import java.nio.file.Path

fun createAvroReader(path: Path): DataFileReader<GenericRecord> {
  val datumReader = GenericDatumReader<GenericRecord>()
  return DataFileReader<GenericRecord>(SeekableFileInput(path.toFile()), datumReader)
}