package io.eels.component.parquet

import io.eels.Row
import io.eels.component.avro.AvroRecordDeserializer
import io.eels.util.map
import io.eels.util.nullTerminatedIterator
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader
import java.util.stream.Stream

/**
 * Creates an Iterator<Row> that will return Row objects for each Parquet GenericRecord.
 * The Row objects returned will have the same schema as the parquet records.
 *
 * @reader the underlying parquet reader to use to load records
 *
 */
object ParquetIterator {
  operator fun invoke(reader: ParquetReader<GenericRecord>): Iterator<Row> = object : Iterator<Row> {

    val iter = Stream.generate { reader.read() }.nullTerminatedIterator().map { AvroRecordDeserializer().toRow(it) }

    override fun hasNext(): Boolean {
      val hasNext = iter.hasNext()
      if (!hasNext) {
        reader.close()
      }
      return hasNext
    }

    override fun next(): Row = iter.next()
  }
}