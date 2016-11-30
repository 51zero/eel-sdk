package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Helper function for create a ParquetWriter using the apache parquet library.
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

  def apply(path: Path, avroSchema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](path)
      .withSchema(avroSchema)
      .withCompressionCodec(compressionCodec)
      .withPageSize(pageSize)
      .withRowGroupSize(blockSize)
      .build()
}



