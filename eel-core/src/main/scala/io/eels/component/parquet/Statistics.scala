package io.eels.component.parquet

case class Statistics(count: Long, compressedSize: Long, uncompressedSize: Long)

object Statistics {
  val Empty = Statistics(0, 0, 0)
}