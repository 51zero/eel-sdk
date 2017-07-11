package io.eels.component.hive

import io.eels.Row
import org.apache.hadoop.fs.Path

trait HiveOutputStream {
  def path: Path
  def records: Int
  def write(row: Row): Unit
  def close(): Unit
}