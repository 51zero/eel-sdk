package io.eels.component.parquet

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * Will write io.eel Rows out to a given path using an underlying apache parquet writer.
 */
class ParquetRowWriter(val path: Path,
                       avroSchema: Schema,
                       val fs: FileSystem) : Logging {

  val config: Config = ConfigFactory.load()
  val skipCrc = config.getBoolean("eel.parquet.skipCrc").apply {
    logger.info("Parquet writer will skipCrc = $this")
  }

  private val writer = ParquetWriterFns.createWriter(path, avroSchema)

  fun write(record: GenericRecord): Unit {
    writer.write(record)
  }

  fun close(): Unit {
    writer.close()
    if (skipCrc) {
      val crc = Path("." + path.toString() + ".crc")
      logger.debug("Deleting crc $crc")
      if (fs.exists(crc))
        fs.delete(crc, false)
    }
  }
}

