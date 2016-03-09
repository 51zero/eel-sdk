package io.eels.component.avro

import java.nio.file.Path

import com.sksamuel.scalax.io.Using
import io.eels._

import scala.collection.JavaConverters._

case class AvroSource(path: Path) extends Source with Using {

  override def schema: Schema = {
    using(AvroReaderSupport.createReader(path)) { reader =>
      val record = reader.next()
      val columns = record.getSchema.getFields.asScala.map(_.name)
      Schema(columns)
    }
  }

  override def parts: Seq[Part] = {
    val part = new Part {
      def reader: SourceReader = new AvroSourceReader(path)
    }
    Seq(part)
  }
}

class AvroSourceReader(path: Path) extends SourceReader {
  private val reader = AvroReaderSupport.createReader(path)
  override def close(): Unit = reader.close()
  override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
    override def hasNext: Boolean = {
      val hasNext = reader.hasNext
      if (!hasNext)
        reader.close()
      hasNext
    }
    override def next: InternalRow = AvroRecordFn.fromRecord(reader.next)
  }
}
