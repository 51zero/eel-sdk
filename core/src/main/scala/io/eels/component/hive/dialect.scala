package io.eels.component.hive

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.github.tototoshi.csv.{CSVWriter, DefaultCSVFormat}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaGen}
import io.eels.component.parquet.ParquetIterator
import io.eels.{Field, FrameSchema, Row}
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter

trait HiveDialect extends StrictLogging {
  def iterator(path: Path, schema: FrameSchema)(implicit fs: FileSystem): Iterator[Row]
  def writer(schema: FrameSchema, path: Path)(implicit fs: FileSystem): HiveWriter
}

object HiveDialect {
  def apply(format: String): HiveDialect = format match {
    case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => ParquetHiveDialect
    case "org.apache.hadoop.mapred.TextInputFormat" => TextHiveDialect
    case "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" => AvroHiveDialect
    case other => sys.error("Unknown hive input format: " + other)
  }
}

trait HiveWriter {
  def write(row: Row): Unit
  def close(): Unit
}

object TextHiveDialect extends HiveDialect with StrictLogging {

  val delimiter = '\u0001'

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val in = fs.open(path)
    lazy val iter = lineIterator(in)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val fields = iter.next.split(delimiter).map(Field.apply).toList.padTo(schema.columns.size, null)
      logger.debug("Fields=" + fields)
      Row(schema.columns, fields)
    }
  }

  def lineIterator(in: InputStream): Iterator[String] = {
    val buff = new BufferedReader(new InputStreamReader(in))
    Iterator.continually(buff.readLine).takeWhile(_ != null)
  }

  override def writer(schema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = new HiveWriter {
    logger.debug(s"Creating text writer for $path with delimiter=${TextHiveDialect.delimiter}")

    val csv = CSVWriter.open(fs.create(path, false))(new DefaultCSVFormat {
      override val delimiter: Char = TextHiveDialect.delimiter
      override val lineTerminator: String = "\n"
    })

    override def write(row: Row): Unit = {
      csv.writeRow(row.fields.map(_.value))
    }

    override def close(): Unit = csv.close()
  }
}

object AvroHiveDialect extends HiveDialect with StrictLogging {

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = {

    logger.debug(s"Creating avro iterator for $path")

    val in = fs.open(path)
    val bytes = IOUtils.toByteArray(in)
    in.close()

    val datumReader = new GenericDatumReader[GenericRecord]()
    val reader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datumReader)

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

object ParquetHiveDialect extends HiveDialect with StrictLogging {

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val iter = ParquetIterator(path)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val map = iter.next.toMap
      val fields = for ( column <- schema.columns ) yield Field(map.getOrElse(column.name, null))
      Row(schema.columns, fields)
    }
  }

  override def writer(schema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    logger.debug(s"Creating parquet writer for $path")
    val avroSchema = AvroSchemaGen(schema)
    val writer = new AvroParquetWriter[GenericRecord](path, avroSchema)
    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: Row): Unit = {
        val record = new Record(avroSchema)
        for ( (key, value) <- row.toMap ) {
          record.put(key, value)
        }
        writer.write(record)
      }
    }
  }
}