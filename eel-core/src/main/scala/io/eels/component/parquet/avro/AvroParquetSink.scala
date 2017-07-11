package io.eels.component.parquet.avro

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.component.avro.{AvroSchemaFns, RecordSerializer}
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroParquetSink {
  def apply(path: String)(implicit fs: FileSystem): AvroParquetSink = AvroParquetSink(new Path(path))
}

case class AvroParquetSink(path: Path, overwrite: Boolean = false)(implicit fs: FileSystem) extends Sink with Logging {

  def withOverwrite(overwrite: Boolean): AvroParquetSink = copy(overwrite = overwrite)

  override def open(schema: StructType): SinkWriter = new SinkWriter {

    private val config = ConfigFactory.load()
    private val caseSensitive = config.getBoolean("eel.parquet.caseSensitive")

    if (overwrite && fs.exists(path))
      fs.delete(path, false)

    private val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = caseSensitive)
    private val writer = new AvroParquetRowWriter(path, avroSchema)
    private val serializer = new RecordSerializer(avroSchema)

    override def write(row: Row): Unit = {
      this.synchronized {
        val record = serializer.serialize(row)
        writer.write(record)
      }
    }

    override def close(): Unit = {
      writer.close()
    }
  }
}

