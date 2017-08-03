package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.Rec
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode.RoundingMode

// implementation of WriteSupport for Row's used by the native ParquetWriter
class RowWriteSupport(messageType: MessageType,
                      roundingMode: RoundingMode,
                      metadata: Map[String, String]) extends WriteSupport[Rec] with Logging {
  logger.trace(s"Created parquet row write support for message type $messageType")

  private var writer: RowWriter = _
  private val schema = ParquetSchemaFns.fromParquetMessageType(messageType)

  override def finalizeWrite(): FinalizedWriteContext = new FinalizedWriteContext(metadata.asJava)

  def init(configuration: Configuration): WriteSupport.WriteContext = {
    new WriteSupport.WriteContext(messageType, new java.util.HashMap())
  }

  def prepareForWrite(record: RecordConsumer) {
    writer = new RowWriter(record, schema, roundingMode)
  }

  def write(row: Rec) {
    writer.write(row)
  }
}

class RowWriter(record: RecordConsumer, schema: StructType, roundingMode: RoundingMode) {

  def write(row: Rec): Unit = {
    record.startMessage()
    val writer = new StructRecordWriter(schema, roundingMode, false)
    writer.write(record, row)
    record.endMessage()
  }
}
