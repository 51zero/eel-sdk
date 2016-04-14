package io.eels

interface Sink {
  fun writer(schema: Schema): SinkWriter
}

interface SinkWriter {
  fun write(row: Row): Unit
  fun close(): Unit
}