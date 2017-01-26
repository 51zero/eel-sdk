package io.eels.component.orc

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.orc.Reader

object OrcBatchIterator extends Logging {

  def apply(reader: Reader,
            fileSchema: StructType,
            projection: Seq[String] = Nil): Iterator[Seq[Row]] = new Iterator[Seq[Row]] {

    val options = new Reader.Options()

    // if we have a projection then we need to return a schema that matches
    // the projection and not the full file schema
    val schema = if (projection.isEmpty) fileSchema else {
      val fields = projection.flatMap(name => fileSchema.field(name))
      StructType(fields)
    }
    logger.info(s"Orc read will use projection=$schema")

    // a projection is column index based, so the given projection columns must be
    // resolved against the file schema to work out which column indices are required
    if (projection.nonEmpty) {
      // we have to include a true for the containing struct itself
      val includes = true +: fileSchema.fieldNames.map(projection.contains)
      logger.debug(s"Setting included columns=${includes.mkString(",")}")
      options.include(includes.toArray)
    }

    val batch = reader.getSchema().createRowBatch()
    val rows = reader.rows(options)
    val vector = new StructColumnVector(batch.numCols, batch.cols: _*)

    val projectionIndices = schema.fields.map(fileSchema.indexOf)
    val deserializer = new StructDeserializer(schema.fields, projectionIndices)

    override def hasNext(): Boolean = rows.nextBatch(batch)

    override def next(): Seq[Row] = {
      val rows = Vector.newBuilder[Row]
      for (rowIndex <- 0 until batch.size) {
        val values = deserializer.readFromVector(rowIndex, vector)
        val row = Row.fromSeq(schema, values)
        rows += row
      }
      rows.result()
    }
  }
}
