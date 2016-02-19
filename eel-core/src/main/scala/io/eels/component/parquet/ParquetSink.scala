package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

case class ParquetSink(path: Path) extends Sink with StrictLogging {
  logger.debug(s"Sink will write to $path")

  override def writer: Writer = new Writer {

    var writer: AvroParquetWriter[GenericRecord] = null
    var avroSchema: Schema = null

    def ensureWriterCreated(row: Row): Unit = {
      if (writer == null) {
        writer = new AvroParquetWriter[GenericRecord](path, avroSchema)
      }
    }

    override def close(): Unit = writer.close()

    override def write(row: Row, schema: FrameSchema): Unit = {
      avroSchema = AvroSchemaGen(schema)
      ensureWriterCreated(row)
      val record = AvroRecordFn.toRecord(row, avroSchema)
      writer.write(record)
    }
  }
}