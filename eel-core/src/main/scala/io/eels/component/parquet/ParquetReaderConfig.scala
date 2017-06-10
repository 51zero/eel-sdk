package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}

case class ParquetReaderConfig(parallelism: Int)

object ParquetReaderConfig extends Logging {

  def apply(): ParquetReaderConfig = apply(ConfigFactory.load())
  def apply(config: Config): ParquetReaderConfig = {

    val parallelism: Int = config.getInt("eel.parquet.parallelism")
    logger.debug(s"Parquet readers will use parallelism = $parallelism")

    ParquetReaderConfig(parallelism)
  }
}