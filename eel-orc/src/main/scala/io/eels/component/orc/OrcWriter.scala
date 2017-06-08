package io.eels.component.orc

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// performs the actual write out of orc data, to be used by an orc sink
class OrcWriter(path: Path,
                structType: StructType,
                bloomFilterColumns: Seq[String],
                rowIndexStride: Option[Int],
                config: OrcSinkConfig)(implicit conf: Configuration) extends Logging {

  private val schema: TypeDescription = OrcSchemaFns.toOrcSchema(structType)
  logger.debug(s"Creating orc writer for schema $schema")

  private val batchSize = {
    val size = ConfigFactory.load().getInt("eel.orc.sink.batchSize")
    Math.max(Math.min(1024, size), 1)
  }
  logger.info(s"Orc writer will use batchsize=$batchSize")

  private val buffer = new ArrayBuffer[Row](batchSize)
  private val serializers = schema.getChildren.asScala.map(OrcSerializer.forType).toArray
  private val batch = schema.createRowBatch(batchSize)

  OrcConf.COMPRESSION_STRATEGY.setString(conf, config.compressionStrategy.name)
  OrcConf.COMPRESS.setString(conf, config.compressionKind.name)
  config.encodingStrategy.map(_.name).foreach(OrcConf.ENCODING_STRATEGY.setString(conf, _))
  config.compressionBufferSize.foreach(OrcConf.BUFFER_SIZE.setLong(conf, _))
  private val options = OrcFile.writerOptions(conf).setSchema(schema)

  rowIndexStride.foreach { size =>
    options.rowIndexStride(size)
    logger.info(s"Using stride size = $size")
  }

  if (bloomFilterColumns.nonEmpty) {
    options.bloomFilterColumns(bloomFilterColumns.mkString(","))
    logger.info(s"Using bloomFilterColumns = $bloomFilterColumns")
  }

  private lazy val writer = OrcFile.createWriter(path, options)

  private val _records = new AtomicInteger(0)

  def write(row: Row): Unit = {
    buffer.append(row)
    if (buffer.size == batchSize)
      flush()
  }

  def records: Int = _records.get()

  def flush(): Unit = {

    def writecol[T <: ColumnVector](rowIndex: Int, colIndex: Int, row: Row): Unit = {
      val value = row.values(colIndex)
      val vector = batch.cols(colIndex).asInstanceOf[T]
      val serializer = serializers(colIndex).asInstanceOf[OrcSerializer[T]]
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
    _records.updateAndGet(new IntUnaryOperator {
      override def applyAsInt(operand: Int): Int = operand + batch.size
    })
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
