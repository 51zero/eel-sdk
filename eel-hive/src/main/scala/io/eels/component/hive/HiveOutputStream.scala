package io.eels.component.hive

import io.eels.Rec
import org.apache.hadoop.fs.Path

trait HiveOutputStream {
  def path: Path
  def records: Int
  def write(row: Rec): Unit
  def close(): Unit
}