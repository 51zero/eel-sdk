package io.eels.component.orc

import io.eels.Row
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.Reader

object OrcBatchIterator {

  def apply(reader: Reader): Iterator[List[Row]] = new Iterator[List[Row]] {

    val batch = reader.getSchema().createRowBatch()
    val schema = OrcFns.readSchema(reader.getSchema)
    val rows = reader.rows()

    override def hasNext(): Boolean = rows.nextBatch(batch)

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
  }
}
