package io.eels.component.hive

trait FilenameStrategy {
  def filename: String
}

object DefaultFilenameStrategy extends FilenameStrategy {
  override def filename: String = "eel_" + System.nanoTime()
}
