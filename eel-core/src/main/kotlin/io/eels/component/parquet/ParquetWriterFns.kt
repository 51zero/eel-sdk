package io.eels.component.parquet

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object ParquetWriterFns : Logging {

  private fun Config.getIntOrElse(key: String, default: Int): Int = if (config.hasPath(key)) this.getInt(key) else default

  val config: Config = ConfigFactory.load()

  val blockSize: Int = config.getIntOrElse("eel.parquet.blockSize", ParquetWriter.DEFAULT_BLOCK_SIZE).apply {
    ParquetReaderSupport.logger.debug("Parquet writer will use blockSize = $this")
  }
  val pageSize: Int = config.getIntOrElse("eel.parquet.pageSize", ParquetWriter.DEFAULT_PAGE_SIZE).apply {
    ParquetReaderSupport.logger.debug("Parquet writer will use pageSize = $this")
  }

  val compressionCodec: CompressionCodecName by lazy {
    val codec = when (config.getString("eel.parquet.compressionCodec").toLowerCase()) {
      "gzip" -> CompressionCodecName.GZIP
      "lzo" -> CompressionCodecName.LZO
      "snappy" -> CompressionCodecName.SNAPPY
      else -> CompressionCodecName.UNCOMPRESSED
    }
    logger.debug("Parquet writer will use compression codec = $codec")
    codec
  }

  fun createWriter(path: Path, avroSchema: Schema): ParquetWriter<GenericRecord> =
    AvroParquetWriter.builder<GenericRecord>(path)
      .withSchema(avroSchema)
      .withCompressionCodec(compressionCodec)
      .withPageSize(pageSize)
      .withRowGroupSize(blockSize)
      .build()
}



