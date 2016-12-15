package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.{CloseableIterator, Part, Row}
import org.apache.hadoop.fs.Path

class ParquetPart(path: Path,
                  predicate: Option[Predicate]) extends Part with Logging {

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    val reader = ParquetReaderFn(path, predicate, None)
    val deser = new ParquetDeserializer()

    override def close(): Unit = {
      super.close()
      reader.close()
    }

    override val iterator: Iterator[Seq[Row]] =
      ParquetIterator(reader).grouped(10000).withPartial(true)
  }
}
