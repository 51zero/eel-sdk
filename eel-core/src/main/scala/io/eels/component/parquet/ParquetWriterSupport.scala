package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

trait ParquetWriterSupport extends StrictLogging {

  val config = ConfigFactory.load()
  val ParquetBlockSizeKey = "eel.parquet.blockSize"
  val ParquetPageSizeKey = "eel.parquet.pageSize"

  protected def compressionCodec: CompressionCodecName = {
    val codec = config.getString("eel.parquet.compressionCodec").toLowerCase match {
      case "gzip" => CompressionCodecName.GZIP
      case "lzo" => CompressionCodecName.LZO
      case "snappy" => CompressionCodecName.SNAPPY
      case _ => CompressionCodecName.UNCOMPRESSED
    }
    logger.debug(s"Parquet writer will use compression codec = $codec")
    codec
  }

  protected def blockSize: Int = {
    val blockSize = if (config.hasPath(ParquetBlockSizeKey)) config.getInt(ParquetBlockSizeKey)
    else org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE
    logger.debug(s"Parquet writer will use blockSize = $blockSize")
    blockSize
  }

  protected def pageSize: Int = {
    val pageSize = if (config.hasPath(ParquetPageSizeKey)) config.getInt(ParquetPageSizeKey)
    else org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE
    logger.debug(s"Parquet writer will use pageSize = $pageSize")
    pageSize
  }

  protected def createParquetWriter(path: Path, avroSchema: Schema): ParquetWriter[GenericRecord] = {
    AvroParquetWriter.builder[GenericRecord](path)
      .withSchema(avroSchema)
      .withCompressionCodec(compressionCodec)
      .withPageSize(pageSize)
      .withRowGroupSize(blockSize)
      .build()
  }
}



