package io.eels.component.orc

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.{OrcFile, TypeDescription}

import scala.collection.mutable.ArrayBuffer

class OrcWriter(path: Path, schema: StructType)(implicit conf: Configuration) extends Logging {

  private val orcSchema: TypeDescription = OrcFns.writeSchema(schema)
  logger.debug(s"Creating orc writer $orcSchema")

  private val config = ConfigFactory.load()
  private val batchSize = {
    val size = config.getInt("eel.orc.sink.batchSize")
    Math.max(Math.min(1024, size), 1)
  }
  logger.info(s"Orc writer will use batchsize=$batchSize")

  private val batch = orcSchema.createRowBatch(batchSize)
  // todo support other column types
  private val cols = batch.cols.map(_.asInstanceOf[BytesColumnVector])
  private val colRange = cols.indices
  private val buffer = new ArrayBuffer[Row](batchSize)

  private lazy val writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(orcSchema))

  def write(row: Row): Unit = {
    buffer.append(row)
    if (buffer.size == batchSize)
      flush()
  }

  def flush(): Unit = {
    // todo extract this into some orc serializer so its easier to read
    // don't use foreach here, using old school for loops for perf
    for (k <- buffer.indices) {
      val row = buffer(k)
      for (j <- colRange) {
        val value = row.values(j)
        val col = cols(j)
        val bytes = if (value == null) Array.emptyByteArray else value.toString.getBytes("UTF8")
        col.setRef(k, bytes, 0, bytes.length)
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
