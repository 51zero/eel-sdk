package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.AvroSchemaGen
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

case class ParquetSink(path: Path) extends Sink with StrictLogging {
  logger.debug(s"Sink will write to $path")

  override def writer: Writer = new Writer {

    var writer: AvroParquetWriter[GenericRecord] = null
    var schema: Schema = null

    def createWriter(row: Row): Unit = {
      if (writer == null) {
        schema = AvroSchemaGen(FrameSchema(row.columns))
        writer = new AvroParquetWriter[GenericRecord](path, schema)
      }
    }

    override def close(): Unit = writer.close()

    override def write(row: Row): Unit = {
      createWriter(row)
      val record = new Record(schema)
      row.toMap.foreach { case (key, value) => record.put(key, value) }
      writer.write(record)
    }
  }
}