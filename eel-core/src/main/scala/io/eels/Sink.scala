package io.eels

import io.eels.schema.Schema

trait Sink {
  def writer(schema: Schema): SinkWriter
}

trait SinkWriter {
  def write(row: Row): Unit
  def close(): Unit
}