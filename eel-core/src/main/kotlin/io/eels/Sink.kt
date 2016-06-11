package io.eels

import io.eels.schema.Schema

interface Sink {
  fun writer(schema: Schema): SinkWriter
}

interface SinkWriter {
  fun write(row: Row): Unit
  fun close(): Unit
}