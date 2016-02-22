package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

case class ParquetSink(path: Path) extends Sink with StrictLogging {
  override def writer: Writer = new ParquetWriter(path)
}

class ParquetWriter(path: Path) extends Writer with ParquetWriterSupport with StrictLogging {

  logger.debug(s"Parquet will write to $path")
  var writer: AvroParquetWriter[GenericRecord] = _
  var avroSchema: Schema = _

  override def close(): Unit = {
    if (writer != null) writer.close()
  }

  override def write(row: Row, schema: FrameSchema): Unit = {
    this.synchronized {
      if (writer == null) {
        avroSchema = AvroSchemaGen(schema)
        writer = createParquetWriter(path, avroSchema)
      }
      val record = AvroRecordFn.toRecord(row, avroSchema)
      writer.write(record)
    }
  }
}