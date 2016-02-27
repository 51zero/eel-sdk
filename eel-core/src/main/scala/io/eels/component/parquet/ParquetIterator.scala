package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.InternalRow
import io.eels.component.avro.AvroRecordFn
import org.apache.hadoop.fs.Path

object ParquetIterator extends StrictLogging {

  def apply(path: Path, columns: Seq[String]): Iterator[InternalRow] = {

    lazy val reader = ParquetReaderSupport.createReader(path, columns)
    lazy val iter = Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      if (columns.isEmpty) AvroRecordFn.fromRecord(record) else AvroRecordFn.fromRecord(record, columns)
    }

    new Iterator[InternalRow] {
      override def hasNext: Boolean = {
        val hasNext = iter.hasNext
        if (!hasNext) {
          logger.debug("Closing parquet iterator")
          reader.close()
        }
        hasNext
      }
      override def next(): InternalRow = iter.next()
    }
  }
}
