package io.eels.component.parquet

import java.util

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.Row
import io.eels.schema.{DataType, DoubleType, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.MessageType

/**
  * Helper function for create a native ParquetWriter using the apache parquet library.
  * Uses config keys to support compression codec, page size, and block size.
  *
  * @param path   the path to save the file at
  * @param schema the schema to use for this file. This schema is the one that will be set, regardless
  *               of the schema set in the data.
  */
object ParquetWriterFn extends Logging {

  implicit class RichConfig(config: Config) {
    def getIntOrElse(key: String, default: Int): Int = if (config.hasPath(key)) config.getInt(key) else default
  }

  private val config: Config = ConfigFactory.load()

  private val blockSize: Int = config.getIntOrElse("eel.parquet.blockSize", ParquetWriter.DEFAULT_BLOCK_SIZE)
  logger.debug(s"Parquet writer will use blockSize = $blockSize")

  private val pageSize: Int = config.getIntOrElse("eel.parquet.pageSize", ParquetWriter.DEFAULT_PAGE_SIZE)
  logger.debug(s"Parquet writer will use pageSize = $pageSize")

  private lazy val compressionCodec = config.getString("eel.parquet.compressionCodec").toLowerCase() match {
    case "gzip" => CompressionCodecName.GZIP
    case "lzo" => CompressionCodecName.LZO
    case "snappy" => CompressionCodecName.SNAPPY
    case _ => CompressionCodecName.UNCOMPRESSED
  }
  logger.debug(s"Parquet writer will use compressionCodec = $compressionCodec")

  val enableDictionary = true
  val validating = true

  def apply(path: Path, schema: StructType): ParquetWriter[Row] = {
    val parquetSchema = ParquetSchemaFns.toParquetSchema(schema)
    new RowParquetWriterBuilder(path, parquetSchema)
      .withCompressionCodec(compressionCodec)
      .withDictionaryEncoding(enableDictionary)
      .withRowGroupSize(blockSize)
      .withPageSize(pageSize)
      .withWriteMode(ParquetFileWriter.Mode.CREATE)
      .withValidation(validating)
      .build()
  }
}

class RowParquetWriterBuilder(path: Path, schema: MessageType)
  extends ParquetWriter.Builder[Row, RowParquetWriterBuilder](path) {
  override def getWriteSupport(conf: Configuration): WriteSupport[Row] = new RowWriteSupport(schema)
  override def self(): RowParquetWriterBuilder = this
}

class RowWriteSupport(schema: MessageType) extends WriteSupport[Row] {

  private var writer: RowWriter = _

  def init(configuration: Configuration): WriteSupport.WriteContext = {
    new WriteSupport.WriteContext(schema, new util.HashMap())
  }

  def prepareForWrite(record: RecordConsumer) {
    writer = new RowWriter(record)
  }

  def write(row: Row) {
    writer.write(row)
  }
}

class RowWriter(record: RecordConsumer) {

  def write(row: Row): Unit = {
    record.startMessage()
    writeRow(row)
    record.endMessage()
  }

  private def writeRow(row: Row): Unit = {
    row.schema.fields.zipWithIndex.foreach { case (field, pos) =>
      record.startField(field.name, pos)
      val writer = ParquetValueWriter(field.dataType)
      writer.write(record, row.get(pos))
      record.endField(field.name, pos)
    }
  }
}

// accepts a scala/java value and writes it out to a record consumer as the appropriate
// parquet value for the given schema type
trait ParquetValueWriter {
  def write(record: RecordConsumer, value: Any)
}

object ParquetValueWriter {
  def apply(dataType: DataType): ParquetValueWriter = {
    dataType match {
      case StringType => StringParquetValueWriter
      case DoubleType => DoubleParquetValueWriter
    }
  }
}

object StringParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addBinary(Binary.fromString(value.toString))
  }
}

object DoubleParquetValueWriter extends ParquetValueWriter {
  override def write(record: RecordConsumer, value: Any): Unit = {
    record.addDouble(value.asInstanceOf[Double])
  }
}