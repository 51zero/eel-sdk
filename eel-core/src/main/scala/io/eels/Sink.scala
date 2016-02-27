package io.eels

trait Sink {
  def writer: Writer
}

trait Writer {
  def write(row: InternalRow, schema: Schema): Unit
  def close(): Unit
}