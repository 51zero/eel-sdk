package io.eels.actions

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory

trait Action extends Logging {
  val config = ConfigFactory.load()
  val requestSize = config.getInt("eel.execution.requestSize")
}