package io.eels.component.parquet

import io.eels.Row
import io.eels.component.avro.avroRecordToRow
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
fun parquetRowIterator(reader: ParquetReader<GenericRecord>): Iterator<Row> = object : Iterator<Row> {

  val iter = Stream.generate { reader.read() }.nullTerminatedIterator().map { avroRecordToRow(it) }

  override fun hasNext(): Boolean {
    val hasNext = iter.hasNext()
    if (!hasNext) {
      reader.close()
    }
    return hasNext
  }

  override fun next(): Row = iter.next()
}