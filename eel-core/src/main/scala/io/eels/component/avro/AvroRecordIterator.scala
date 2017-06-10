package io.eels.component.avro

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericRecord

object AvroRecordIterator {
  def apply(reader: DataFileReader[GenericRecord]): Iterator[GenericRecord] = new Iterator[GenericRecord] {
    override def hasNext: Boolean = reader.hasNext
    override def next(): GenericRecord = reader.next()
  }
}
