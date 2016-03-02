package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn}
import io.eels.{InternalRow, Schema, Sink, SinkWriter}
import org.apache.avro.{Schema => AvroSchema}
import org.apache.hadoop.fs.{FileSystem, Path}

case class ParquetSink(path: Path)(implicit fs: FileSystem) extends Sink with StrictLogging {
  override def writer(schema: Schema): SinkWriter = new ParquetSinkWriter(schema, path)
}

class ParquetSinkWriter(schema: Schema, path: Path)
                       (implicit fs: FileSystem) extends SinkWriter with StrictLogging {

  logger.debug(s"Parquet will write to $path")
  private val config = ConfigFactory.load()
  private val avroSchema = AvroSchemaFn.toAvro(schema)
  private val writer = RollingParquetWriter(path, avroSchema)

  override def close(): Unit = writer.close()

  override def write(row: InternalRow): Unit = {
    this.synchronized {
      val record = AvroRecordFn.toRecord(row, avroSchema, schema, config)
      writer.write(record)
    }
  }
}