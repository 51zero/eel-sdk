package io.eels.component.jdbc

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.schema.{Precision, Scale}

case class JdbcReaderConfig(defaultPrecision: Precision, defaultScale: Scale)

object JdbcReaderConfig extends Logging {

  def apply(): JdbcReaderConfig = apply(ConfigFactory.load())
  def apply(config: Config): JdbcReaderConfig = {

    val defaultPrecision: Int = config.getInt("eel.jdbc.oracle.default-precision")
    val defaultScale: Int = config.getInt("eel.jdbc.oracle.default-scale")

    JdbcReaderConfig(Precision(defaultPrecision), Scale(defaultScale))
  }
}
