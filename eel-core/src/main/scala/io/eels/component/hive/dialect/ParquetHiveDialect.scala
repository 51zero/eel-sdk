package io.eels.component.hive.dialect

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet.{ParquetIterator, ParquetLogMute, ParquetReaderSupport, RollingParquetWriter}
import io.eels.{InternalRow, Schema, SourceReader}
import org.apache.hadoop.fs.{FileSystem, Path}

object ParquetHiveDialect extends HiveDialect with StrictLogging {

  private val config = ConfigFactory.load()

  override def writer(schema: Schema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    ParquetLogMute()

    val avroSchema = AvroSchemaFn.toAvro(schema)
    val writer = RollingParquetWriter(path, avroSchema)

    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: InternalRow): Unit = {
        val record = AvroRecordFn.toRecord(row, avroSchema, schema, config)
        logger.trace(record.toString)
        writer.write(record)
      }
    }
  }
  override def reader(path: Path, schema: Schema, columnNames: Seq[String])
                     (implicit fs: FileSystem): SourceReader = new SourceReader {
    val reader = ParquetReaderSupport.createReader(path, columnNames)
    override def close(): Unit = reader.close()
    override def iterator: Iterator[InternalRow] = ParquetIterator(reader, columnNames)
  }
}
