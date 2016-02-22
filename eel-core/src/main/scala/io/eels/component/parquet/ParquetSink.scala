package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import io.eels.{FrameSchema, InternalRow, Sink, Writer}
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileSystem, Path}

case class ParquetSink(path: Path)(implicit fs: FileSystem) extends Sink with StrictLogging {
  override def writer: Writer = new ParquetWriter(path)
}

class ParquetWriter(path: Path)
                   (implicit fs: FileSystem) extends Writer with RollingParquetWriterSupport with StrictLogging {

  logger.debug(s"Parquet will write to $path")
  var writer: RollingParquetWriter = _
  var avroSchema: Schema = _

  override def close(): Unit = {
    if (writer != null) writer.close()
  }

  override def write(row: InternalRow, schema: FrameSchema): Unit = {
    this.synchronized {
      if (writer == null) {
        avroSchema = AvroSchemaGen(schema)
        writer = createRollingParquetWriter(path, avroSchema)
      }
      val record = AvroRecordFn.toRecord(row, avroSchema, schema)
      writer.write(record)
    }
  }
}