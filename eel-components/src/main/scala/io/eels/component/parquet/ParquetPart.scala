package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.component.parquet.util.ParquetIterator
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import com.sksamuel.exts.OptionImplicits._

class ParquetPart(path: Path,
                  predicate: Option[Predicate],
                  projection: Seq[String])
                 (implicit conf: Configuration) extends Part with Logging with Using {

  def projectionSchema = {
    if (projection.isEmpty)
      None
    else {
      val messageType = ParquetFileReader.open(conf, path).getFileMetaData.getSchema
      val structType = ParquetSchemaFns.fromParquetGroupType(messageType)
      val projected = StructType(structType.fields.filter(field => projection.contains(field.name)))
      ParquetSchemaFns.toParquetSchema(projected).some
    }
  }

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    val reader = ParquetReaderFn(path, predicate, projectionSchema)

    override def close(): Unit = {
      super.close()
      reader.close()
    }

    override val iterator: Iterator[Seq[Row]] = ParquetIterator(reader).grouped(1000).withPartial(true)
  }
}