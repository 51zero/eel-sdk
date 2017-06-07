package io.eels.component.parquet.avro

import com.sksamuel.exts.Logging
import io.eels.component.avro.AvroDeserializer
import io.eels.component.parquet.util.ParquetIterator
import io.eels.{CloseableIterator, Part, Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class AvroParquetPart(path: Path,
                      predicate: Option[Predicate])(implicit conf: Configuration) extends Part with Logging {

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    val reader = AvroParquetReaderFn(path, predicate, None)
    val deser = new AvroDeserializer()

    override def close(): Unit = {
      super.close()
      reader.close()
    }

    override val iterator: Iterator[Seq[Row]] =
      ParquetIterator(reader).grouped(1000).withPartial(true).map(rows => rows.map(deser.toRow))
  }
}
