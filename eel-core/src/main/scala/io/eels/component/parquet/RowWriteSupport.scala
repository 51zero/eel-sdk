package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.Row
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConverters._

// implementation of WriteSupport for Row's used by the native ParquetWriter
class RowWriteSupport(schema: MessageType,
                      metadata: Map[String, String]) extends WriteSupport[Row] with Logging {
  logger.debug(s"Created parquet row write support for schema message type $schema")

  private var writer: RowWriter = _

  override def finalizeWrite(): FinalizedWriteContext = new FinalizedWriteContext(metadata.asJava)

  def init(configuration: Configuration): WriteSupport.WriteContext = {
    new WriteSupport.WriteContext(schema, new java.util.HashMap())
  }

  def prepareForWrite(record: RecordConsumer) {
    writer = new RowWriter(record)
  }

  def write(row: Row) {
    writer.write(row)
  }
}

class RowWriter(record: RecordConsumer) {

  def write(row: Row): Unit = {
    record.startMessage()
    val writer = new StructWriter(row.schema, false)
    writer.write(record, row.values)
    record.endMessage()
  }
}
