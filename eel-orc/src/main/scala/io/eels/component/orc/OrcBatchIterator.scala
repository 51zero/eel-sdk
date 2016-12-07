package io.eels.component.orc

import com.sksamuel.exts.Logging
import io.eels.{CloseableIterator, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.OrcFile
import org.apache.orc.OrcFile.ReaderOptions

object OrcBatchIterator extends Logging {

  def apply(path: Path)
           (implicit conf: Configuration): CloseableIterator[List[Row]] = new CloseableIterator[List[Row]] {

    val reader = OrcFile.createReader(path, new ReaderOptions(conf))
    val schema = OrcFns.readSchema(reader.getSchema)
    val batch = reader.getSchema().createRowBatch()
    val rows = reader.rows()
    var closed = false

    override def hasNext(): Boolean = !closed && rows.nextBatch(batch)

    override def next(): List[Row] = {
      val cols = batch.cols.map(_.asInstanceOf[BytesColumnVector])
      val rows = for (k <- 0 until batch.size) yield {
        val values = cols.map {
          col =>
            val bytes = col.vector.head.slice(col.start(k), col.start(k) + col.length(k))
            new String(bytes, "UTF8")
        }
        Row(schema, values.toVector)
      }
      rows.toList
    }

    override def close(): Unit = {
      closed = true
    }
  }
}
