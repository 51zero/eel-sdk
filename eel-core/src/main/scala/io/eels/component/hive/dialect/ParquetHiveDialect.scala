package io.eels.component.hive.dialect

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroSchemaFn, DefaultAvroRecordMarshaller}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet.{ParquetIterator, ParquetLogMute, ParquetReaderSupport, RollingParquetWriter}
import io.eels.{InternalRow, Schema, SourceReader}
import org.apache.hadoop.fs.{FileSystem, Path}

object ParquetHiveDialect extends HiveDialect with StrictLogging {

  override def writer(schema: Schema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    ParquetLogMute()

    // hive is case insensitive so we must lower case everything to keep it consistent
    val avroSchema = AvroSchemaFn.toAvro(schema, caseSensitive = false)
    val writer = RollingParquetWriter(path, avroSchema)
    val marshaller = new DefaultAvroRecordMarshaller(schema, avroSchema)

    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: InternalRow): Unit = {
        val record = marshaller.toRecord(row)
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
