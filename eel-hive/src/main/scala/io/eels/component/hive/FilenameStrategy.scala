package io.eels.component.hive

import java.util.UUID

trait FilenameStrategy {
  def tempdir: String
  def filename(discriminator: Option[String]): String
}

object DefaultEelFilenameStrategy extends FilenameStrategy {
  override def filename(discriminator: Option[String]): String = {
    "eel_" + System.nanoTime() + discriminator.map("_" + _.stripPrefix("_")).getOrElse("")
  }
  override def tempdir: String = ".eeltemp_" + UUID.randomUUID.toString
}
