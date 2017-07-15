package io.eels.component.hive

/**
  * Strategy responsible for the filenames created by eel when writing out data.
  */
trait FilenameStrategy {
  def filename: String
}

object DefaultFilenameStrategy extends FilenameStrategy {
  override def filename: String = "eel_" + System.nanoTime()
}
