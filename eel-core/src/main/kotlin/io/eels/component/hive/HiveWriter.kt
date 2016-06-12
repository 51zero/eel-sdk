package io.eels.component.hive

import io.eels.Row

interface HiveWriter {
  fun write(row: Row): Unit
  fun close(): Unit
}