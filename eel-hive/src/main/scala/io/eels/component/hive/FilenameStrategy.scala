package io.eels.component.hive

trait FilenameStrategy {
  def filename: String
}

object DefaultEelFilenameStrategy extends FilenameStrategy {
  override def filename: String = "eel_" + System.nanoTime()
}
