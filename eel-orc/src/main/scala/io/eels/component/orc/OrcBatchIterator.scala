package io.eels.component.orc

import io.eels.Row
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.orc.Reader

object OrcBatchIterator {

  def apply(reader: Reader): Iterator[List[Row]] = new Iterator[List[Row]] {

    val batch = reader.getSchema().createRowBatch()
    val structType = OrcSchemaFns.fromOrcSchema(reader.getSchema)
    val rows = reader.rows()

    override def hasNext(): Boolean = rows.nextBatch(batch)

    override def next(): List[Row] = {

      def readcol[T <: ColumnVector](rowIndex: Int, colIndex: Int): Any = {
        val field = structType.fields(colIndex)
        val deser = OrcDeserializer(field.dataType).asInstanceOf[OrcDeserializer[T]]
        val vector = batch.cols(colIndex).asInstanceOf[T]
        deser.readFromVector(rowIndex, vector)
      }

      val rows = for (rowIndex <- 0 until batch.size) yield {
        val values = for (colIndex <- structType.fields.indices) yield {
          readcol(rowIndex, colIndex)
        }
        Row(structType, values.toVector)
      }
      rows.toList
    }
  }
}
