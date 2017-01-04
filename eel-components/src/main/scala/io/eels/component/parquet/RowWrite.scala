package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.Row
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

class RowWriteSupport(schema: MessageType) extends WriteSupport[Row] with Logging {
  logger.debug(s"Created parquet row write support for schema message type $schema")

  private var writer: RowWriter = _

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
    writeRow(row)
    record.endMessage()
  }

  private def writeRow(row: Row): Unit = {
    row.schema.fields.zipWithIndex.foreach { case (field, pos) =>
      record.startField(field.name, pos)
      val writer = ParquetValueConversion(field.dataType)
      writer.write(record, row.get(pos))
      record.endField(field.name, pos)
    }
  }
}
