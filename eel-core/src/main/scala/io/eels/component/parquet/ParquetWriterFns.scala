package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object ParquetWriterFns extends Logging {

  implicit class RichConfig(config: Config) {
    def getIntOrElse(key: String, default: Int): Int = if (config.hasPath(key)) config.getInt(key) else default
  }

  val config: Config = ConfigFactory.load()

  val blockSize: Int = config.getIntOrElse("eel.parquet.blockSize", ParquetWriter.DEFAULT_BLOCK_SIZE)
  logger.debug("Parquet writer will use blockSize = $this")

  val pageSize: Int = config.getIntOrElse("eel.parquet.pageSize", ParquetWriter.DEFAULT_PAGE_SIZE)
  logger.debug("Parquet writer will use pageSize = $this")

  lazy val compressionCodec: CompressionCodecName = {
    val codec = config.getString("eel.parquet.compressionCodec").toLowerCase() match {
      case "gzip" => CompressionCodecName.GZIP
      case "lzo" => CompressionCodecName.LZO
      case "snappy" => CompressionCodecName.SNAPPY
      case _ => CompressionCodecName.UNCOMPRESSED
    }
    logger.debug("Parquet writer will use compression codec = $codec")
    codec
  }

  def createWriter(path: Path, avroSchema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](path)
      .withSchema(avroSchema)
      .withCompressionCodec(compressionCodec)
      .withPageSize(pageSize)
      .withRowGroupSize(blockSize)
      .build()
}



