package io.eels.component.avro

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{AvroFSInput, FileSystem, Path}

object AvroReaderFns {

  def createAvroReader(path: Path)
                      (implicit conf: Configuration, fs: FileSystem): DataFileReader[GenericRecord] = {
    val datumReader = new GenericDatumReader[GenericRecord]()
    new DataFileReader[GenericRecord](new AvroFSInput(fs.open(path), fs.getFileStatus(path).getLen), datumReader)
  }
}