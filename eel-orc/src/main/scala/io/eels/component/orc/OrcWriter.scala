package io.eels.component.orc

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.orc.{OrcFile, TypeDescription}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class OrcWriter(path: Path, structType: StructType)(implicit conf: Configuration) extends Logging {

  private val schema: TypeDescription = OrcSchemaFns.toOrcSchema(structType)
  logger.debug(s"Creating orc writer for schema $schema")

  private val config = ConfigFactory.load()
  private val batchSize = {
    val size = config.getInt("eel.orc.sink.batchSize")
    Math.max(Math.min(1024, size), 1)
  }
  logger.info(s"Orc writer will use batchsize=$batchSize")

  private val buffer = new ArrayBuffer[Row](batchSize)
  private val serializers = schema.getChildren.asScala.map(OrcSerializer.forType).toArray

  private val batch = schema.createRowBatch(batchSize)
  private lazy val writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema))

  def write(row: Row): Unit = {
    buffer.append(row)
    if (buffer.size == batchSize)
      flush()
  }

  def flush(): Unit = {

    def writecol[T <: ColumnVector](rowIndex: Int, colIndex: Int, row: Row): Unit = {
      val serializer = serializers(colIndex).asInstanceOf[OrcSerializer[T]]
      val vector = batch.cols(colIndex).asInstanceOf[T]
      val value = row.values(colIndex)
      serializer.writeToVector(rowIndex, vector, value)
    }

    // don't use foreach here, using old school for loops for perf
    for (rowIndex <- buffer.indices) {
      val row = buffer(rowIndex)
      for (colIndex <- batch.cols.indices) {
        writecol(rowIndex, colIndex, row)
      }
    }

    batch.size = buffer.size
    writer.addRowBatch(batch)
    buffer.clear()
    batch.reset()
  }

  def close(): Long = {
    if (buffer.nonEmpty)
      flush()
    writer.close()
    val count = writer.getNumberOfRows
    logger.info(s"Orc writer wrote $count rows")
    count
  }
}
