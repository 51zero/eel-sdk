package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import io.eels.Row
import io.eels.Sink
import io.eels.SinkWriter
import io.eels.component.avro.AvroRecordSerializer
import io.eels.component.avro.AvroSchemaFns
import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

data class ParquetSink @JvmOverloads constructor(val path: Path, val fs: FileSystem = FileSystem.get(Configuration())) : Sink, Logging {
  override fun writer(schema: Schema): SinkWriter = object : SinkWriter {

    private val config = ConfigFactory.load()
    private val caseSensitive = config.getBoolean("eel.parquet.caseSensitive")

    private val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = caseSensitive)
    private val writer = ParquetRowWriter(path, avroSchema, fs)
    private val serializer = AvroRecordSerializer(avroSchema)

    override fun write(row: Row) {
      synchronized(this) {
        val record = serializer.toRecord(row)
        writer.write(record)
      }
    }

    override fun close() {
      writer.close()
    }
  }
}

