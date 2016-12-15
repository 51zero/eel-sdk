package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.component.avro.{AvroSchemaFns, RecordSerializer}
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroParquetSink {
  def apply(path: String)(implicit fs: FileSystem): AvroParquetSink = AvroParquetSink(new Path(path))
}

case class AvroParquetSink(path: Path)(implicit fs: FileSystem) extends Sink with Logging {

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    private val config = ConfigFactory.load()
    private val caseSensitive = config.getBoolean("eel.parquet.caseSensitive")

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

case class ParquetSink(path: Path)(implicit fs: FileSystem) extends Sink with Logging {

  override def writer(schema: StructType): SinkWriter = new SinkWriter {

    private val writer = ParquetWriterFn(path, schema)

    override def write(row: Row): Unit = {
      this.synchronized {
        writer.write(row)
      }
    }

    override def close(): Unit = {
      writer.close()
    }
  }
}