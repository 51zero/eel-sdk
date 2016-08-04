package io.eels.component.parquet

import io.eels.Row
import io.eels.component.avro.AvroRecordDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

/**
 * Creates an Iterator<Row> that will return a Row object for each GenericRecord containing in the parquet reader.
 * The Row objects returned will have the same schema as defined in the parquet records.
 *
 * @reader the underlying parquet reader to use to load records
 */
object ParquetRowIterator {
  operator fun invoke(reader: ParquetReader<GenericRecord>): Iterator<Row> =
      generateSequence { reader.read() }.map { AvroRecordDeserializer().toRow(it) }.iterator()
}