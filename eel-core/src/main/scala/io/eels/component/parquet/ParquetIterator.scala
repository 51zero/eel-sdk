package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.InternalRow
import io.eels.component.avro.AvroRecordFn
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

object ParquetIterator extends StrictLogging {

  def apply(reader: ParquetReader[GenericRecord],
            columnNames: Seq[String] = Nil): Iterator[InternalRow] = new Iterator[InternalRow] {

    val iter = Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      if (columnNames.isEmpty) AvroRecordFn.fromRecord(record) else AvroRecordFn.fromRecord(record, columnNames)
    }

    override def hasNext: Boolean = {
      val hasNext = iter.hasNext
      if (!hasNext) {
        reader.close()
      }
      hasNext
    }

    override def next(): InternalRow = iter.next()
  }
}

