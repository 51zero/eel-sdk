package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.component.avro.AvroDeserializer
import io.eels.{CloseableIterator, Part, Row}
import org.apache.hadoop.fs.Path

class AvroParquetPart(path: Path,
                      predicate: Option[Predicate]) extends Part with Logging {

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    val reader = AvroParquetReaderFn(path, predicate, None)
    val iter = ParquetIterator(reader).grouped(100).withPartial(true)
    val deser = new AvroDeserializer()
    var closed = false

    override def next(): Seq[Row] = iter.next.map(deser.toRow)

    override def hasNext(): Boolean = !closed && iter.hasNext

    override def close(): Unit = {
      closed = true
      reader.close()
    }
  }
}
