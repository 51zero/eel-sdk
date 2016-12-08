package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}

trait ReaderFn extends Logging {
  protected val config: Config = ConfigFactory.load()
  protected val parallelism = config.getInt("eel.parquet.parallelism").toString()
  logger.debug(s"Parquet readers will use parallelism = $parallelism")
}
