package io.eels.component.parquet

import io.eels.Row
import io.eels.component.avro.AvroRecordDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

/**
  * Creates an Iterator[Row] that will return a Row object for each GenericRecord
  * contained in the parquet reader. The Row objects returned will have the same
  * schema as defined in the parquet records.
  *
  * @param reader the underlying parquet reader to use to load records
  */
object ParquetRowIterator {
  def apply(reader: ParquetReader[GenericRecord]): Iterator[Row] = {
    val deserializer = new AvroRecordDeserializer()
    Iterator.continually(reader.read).takeWhile(_ != null).map { it => deserializer.toRow(it) }
  }
}