package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.config.ConfigSupport
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

case class ParquetWriterConfig(blockSize: Int,
                               pageSize: Int,
                               compressionCodec: CompressionCodecName,
                               enableDictionary: Boolean,
                               validating: Boolean)

object ParquetWriterConfig extends Logging with ConfigSupport {

  def apply(): ParquetWriterConfig = apply(ConfigFactory.load())
  def apply(config: Config): ParquetWriterConfig = {

    val blockSize: Int = config.getIntOrElse("eel.parquet.blockSize", ParquetWriter.DEFAULT_BLOCK_SIZE)
    logger.debug(s"Parquet writer will use blockSize = $blockSize")

    val pageSize: Int = config.getIntOrElse("eel.parquet.pageSize", ParquetWriter.DEFAULT_PAGE_SIZE)
    logger.debug(s"Parquet writer will use pageSize = $pageSize")

    val compressionCodec = config.getString("eel.parquet.compressionCodec").toLowerCase() match {
      case "gzip" => CompressionCodecName.GZIP
      case "lzo" => CompressionCodecName.LZO
      case "snappy" => CompressionCodecName.SNAPPY
      case _ => CompressionCodecName.UNCOMPRESSED
    }
    logger.debug(s"Parquet writer will use compressionCodec = $compressionCodec")

    ParquetWriterConfig(blockSize, pageSize, compressionCodec, true, true)
  }
}