package io.eels

trait Sink {
  def writer(schema: Schema): SinkWriter
}

trait SinkWriter {
  def write(row: InternalRow): Unit
  def close(): Unit
}