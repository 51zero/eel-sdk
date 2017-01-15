package io.eels.component.hive

import io.eels.Row

trait HiveWriter {
  def records: Int
  def write(row: Row): Unit
  def close(): Unit
}