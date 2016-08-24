package io.eels.component.orc

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.{OrcFile, TypeDescription}

import scala.collection.mutable.ArrayBuffer

class OrcWriter(path: Path, schema: Schema)(implicit conf: Configuration) extends Logging {
  logger.debug(s"Creating orc writer $schema")

  private val buffer = new ArrayBuffer[Row]

  private val config = ConfigFactory.load()
  private val batchSize = {
    val size = config.getInt("eel.orc.sink.batchSize")
    Math.max(Math.min(1024, size), 1)
  }
  logger.info(s"Orc writer will use batchsize=$batchSize")

  private val orcSchema: TypeDescription = OrcFns.writeSchema(schema)
  logger.debug(s"orcSchema=$orcSchema")

  private lazy val writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(orcSchema))

  def write(row: Row): Unit = {
    buffer.append(row)
    if (buffer.size == batchSize)
      flush()
  }

  def flush(): Unit = {
    logger.debug(s"Flushing orc batch (${buffer.size} rows)")
    val batch = orcSchema.createRowBatch(buffer.size)
    val cols = batch.cols.map(_.asInstanceOf[BytesColumnVector])
    for ((row, k) <- buffer.zipWithIndex) {
      for ((value, col) <- row.values.zip(cols)) {
        val bytes = if (value == null) Array.emptyByteArray else value.toString.getBytes("UTF8")
        col.setRef(k, bytes, 0, bytes.length)
      }
    }
    batch.size = buffer.size
    writer.addRowBatch(batch)
    buffer.clear()
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
