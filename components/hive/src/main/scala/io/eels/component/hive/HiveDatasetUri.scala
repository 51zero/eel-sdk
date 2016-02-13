package io.eels.component.hive

case class HiveDatasetUri(db: String, table: String)

object HiveDatasetUri {
  val Regex = "hive:(.*?):(.*?)(\\?.*)?".r
  def apply(str: String): HiveDatasetUri = str match {
    case Regex(db, table, params) => HiveDatasetUri(db, table)
    case _ => sys.error("Invalid hive uri: " + str)
  }
  def unapply(str: String): Option[(String, String)] = str match {
    case Regex(db, table, params) => Some((db, table))
    case _ => None
  }
}
