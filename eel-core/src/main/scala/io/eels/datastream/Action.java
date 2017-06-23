package io.eels.datastream;

trait Action extends Logging {
  val config = ConfigFactory.load()
  val requestSize = config.getInt("eel.execution.requestSize")
}
