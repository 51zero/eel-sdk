package io.eels.component.parquet.avro

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Will write io.eel Rows out to a given path using an underlying apache parquet writer.
  */
class AvroParquetRowWriter(path: Path,
                           avroSchema: Schema)(implicit fs: FileSystem) extends Logging {

  private val config: Config = ConfigFactory.load()
  private val skipCrc = config.getBoolean("eel.parquet.skipCrc")
  logger.info(s"Parquet writer will skipCrc = $skipCrc")

  private val writer = AvroParquetWriterFn.apply(path, avroSchema)

  def write(record: GenericRecord): Unit = {
    writer.write(record)
  }

  def close(): Unit = {
    writer.close()
    if (skipCrc) {
      val crc = new Path("." + path.toString() + ".crc")
      logger.debug("Deleting crc $crc")
      if (fs.exists(crc))
        fs.delete(crc, false)
    }
  }
}

