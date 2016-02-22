package io.eels.component.parquet

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileSystem, Path}

trait RollingParquetWriterSupport extends StrictLogging {

  protected val config = ConfigFactory.load()
  protected val MaxRecordsPerFileKey = "eel.parquet.maxRecordsPerFile"
  protected val MaxFileSizeKey = "eel.parquet.maxFileSize"

  protected def createRollingParquetWriter(path: Path, avroSchema: Schema)
                                          (implicit fs: FileSystem): RollingParquetWriter = {

    val maxRecordsPerFile = config.getInt(MaxRecordsPerFileKey)
    logger.debug(s"Parquet writer will use maxRecordsPerFile = $maxRecordsPerFile")

    val maxFileSize = config.getInt(MaxFileSizeKey)
    logger.debug(s"Parquet writer will use maxFileSize = $maxFileSize")

    new RollingParquetWriter(path, avroSchema, maxFileSize, maxRecordsPerFile)
  }
}
