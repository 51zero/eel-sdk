package io.eels.component.parquet

import io.eels.Row
import io.eels.component.avro.AvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

/**
  * Creates an Iterator[Row] that will return a Row object for each GenericRecord
  * contained in the parquet reader. The Row objects returned will use the schema
  * defined in the parquet records themselves.
  *
  * @param reader the underlying parquet reader to use to load records
  */
object ParquetRowIterator {
  def apply(reader: ParquetReader[GenericRecord]): Iterator[Row] = {
    val deserializer = new AvroDeserializer()
    Iterator.continually(reader.read).takeWhile(_ != null).map { it => deserializer.toRow(it) }
  }
}