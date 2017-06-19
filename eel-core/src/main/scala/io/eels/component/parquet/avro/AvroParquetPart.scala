package io.eels.component.parquet.avro

import com.sksamuel.exts.Logging
import io.eels.component.avro.AvroDeserializer
import io.eels.component.parquet.util.ParquetIterator
import io.eels.{CloseIterator, Part, Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class AvroParquetPart(path: Path,
                      predicate: Option[Predicate])(implicit conf: Configuration) extends Part with Logging {

  /**
    * Returns the data contained in this part in the form of an iterator. This function should return a new
    * iterator on each invocation. The iterator can be lazily initialized to the first read if required.
    */
  override def iterator2(): CloseIterator[Row] = {
    val reader = AvroParquetReaderFn(path, predicate, None)
    val deser = new AvroDeserializer()
    val iterator: Iterator[Row] = ParquetIterator(reader).map(deser.toRow)
    CloseIterator(reader.close _, iterator)
  }
}
