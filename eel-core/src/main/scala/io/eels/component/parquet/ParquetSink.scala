package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.{Row, Sink, SinkWriter}
import io.eels.component.avro.{AvroRecordSerializer, AvroSchemaFns}
import io.eels.schema.Schema
import org.apache.hadoop.fs.{FileSystem, Path}

case class ParquetSink(path: Path)(implicit fs: FileSystem) extends Sink with Logging {

  override def writer(schema: Schema): SinkWriter = new SinkWriter {

    private val config = ConfigFactory.load()
    private val caseSensitive = config.getBoolean("eel.parquet.caseSensitive")

    private val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = caseSensitive)
    private val writer = new ParquetRowWriter(path, avroSchema)
    private val serializer = new AvroRecordSerializer(avroSchema)

    override def write(row: Row): Unit = {
      this.synchronized {
        val record = serializer.toRecord(row)
        writer.write(record)
      }
    }

    override def close(): Unit = {
      writer.close()
    }
  }
}

