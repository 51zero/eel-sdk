package io.eels.component.orc

import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.orc.Reader

object OrcBatchIterator {

  def apply(reader: Reader, fileSchema: StructType): Iterator[Seq[Row]] = new Iterator[Seq[Row]] {

    val batch = reader.getSchema().createRowBatch()
    val rows = reader.rows()
    val deserializers = fileSchema.fields.map(_.dataType).map(OrcDeserializer.apply).toArray

    override def hasNext(): Boolean = rows.nextBatch(batch)

    override def next(): Seq[Row] = {

      def readcol[T <: ColumnVector](rowIndex: Int, colIndex: Int): Any = {
        val deser = deserializers(colIndex).asInstanceOf[OrcDeserializer[T]]
        val vector = batch.cols(colIndex).asInstanceOf[T]
        deser.readFromVector(rowIndex, vector)
      }

      val rows = Vector.newBuilder[Row]
      for (rowIndex <- 0 until batch.size) {
        val builder = Vector.newBuilder[Any]
        for (colIndex <- fileSchema.fields.indices) {
          builder += readcol(rowIndex, colIndex)
        }
        val row = Row(fileSchema, builder.result)
        rows += row
      }
      rows.result()
    }
  }
}
