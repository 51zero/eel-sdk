package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileSystem, Path}

trait RollingParquetWriterSupport extends StrictLogging {

  protected val config = ConfigFactory.load()
  protected val MaxRecordsPerFileKey = "eel.parquet.maxRecordsPerFile"
  protected val MaxFileSizeKey = "eel.parquet.maxFileSize"
  protected val SkipCrcKey = "eel.parquet.skipCrc"
  protected val skipCrc = config.getBoolean(SkipCrcKey)

  protected def createRollingParquetWriter(path: Path, avroSchema: Schema)
                                          (implicit fs: FileSystem): RollingParquetWriter = {
    val maxRecordsPerFile = config.getInt(MaxRecordsPerFileKey)
    val maxFileSize = config.getInt(MaxFileSizeKey)
    val skipCrc = config.getBoolean(SkipCrcKey)
    new RollingParquetWriter(path, avroSchema, maxFileSize, maxRecordsPerFile, skipCrc)
  }
}
