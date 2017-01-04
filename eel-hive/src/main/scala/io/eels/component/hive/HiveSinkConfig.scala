package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}

case class HiveSinkConfig(padNulls: Boolean, includePartitionsInData: Boolean)

object HiveSinkConfig extends Logging {

  def apply(): HiveSinkConfig = apply(ConfigFactory.load())
  def apply(config: Config): HiveSinkConfig = {

    val padNulls = config.getBoolean("eel.hive.sink.pad-with-null")
    logger.debug(s"Parquet readers padNulls = $padNulls")

    val includePartitionsInData = config.getBoolean("eel.hive.sink.include-partitions-in-data")

    HiveSinkConfig(padNulls, includePartitionsInData)
  }
}