package io.eels.component.hive.dialect

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Row, FrameSchema}
import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.{file, generic}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}

object AvroHiveDialect extends HiveDialect with StrictLogging {

  override def iterator(path: Path, schema: FrameSchema, ignored: Seq[String])
                       (implicit fs: FileSystem): Iterator[Row] = {

    logger.debug(s"Creating avro iterator for $path")

    val in = fs.open(path)
    val bytes = IOUtils.toByteArray(in)
    in.close()

    val datumReader = new generic.GenericDatumReader[GenericRecord]()
    val reader = new DataFileReader[GenericRecord](new file.SeekableByteArrayInput(bytes), datumReader)

    new Iterator[Row] {
      override def hasNext: Boolean = reader.hasNext
      override def next(): Row = AvroRecordFn.fromRecord(reader.next)
    }
  }

  override def writer(schema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    logger.debug(s"Creating avro writer for $path")

    val avroSchema = AvroSchemaGen(schema)
    val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    val out = fs.create(path, false)
    val writer = dataFileWriter.create(avroSchema, out)

    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: Row): Unit = {
        val record = AvroRecordFn.toRecord(row, avroSchema)
        writer.append(record)
      }
    }
  }
}
