package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn}
import io.eels.{InternalRow, Schema, Sink, Writer}
import org.apache.avro.{Schema => AvroSchema}
import org.apache.hadoop.fs.{FileSystem, Path}

case class ParquetSink(path: Path)(implicit fs: FileSystem) extends Sink with StrictLogging {

  override def writer: Writer = new ParquetWriter(path)

  class ParquetWriter(path: Path)
                     (implicit fs: FileSystem) extends Writer with StrictLogging {

    logger.debug(s"Parquet will write to $path")
    val config = ConfigFactory.load()
    var writer: RollingParquetWriter = _
    var avroSchema: AvroSchema = _

    override def close(): Unit = {
      if (writer != null) writer.close()
    }

    override def write(row: InternalRow, schema: Schema): Unit = {
      this.synchronized {
        if (writer == null) {
          avroSchema = AvroSchemaFn.toAvro(schema)
          writer = RollingParquetWriter(path, avroSchema)
        }
        val record = AvroRecordFn.toRecord(row, avroSchema, schema, config)
        writer.write(record)
      }
    }
  }
}
