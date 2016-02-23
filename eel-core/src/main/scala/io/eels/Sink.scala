package io.eels

trait Sink {
  def writer: Writer
}

trait Writer {
  def write(row: InternalRow, schema: FrameSchema): Unit
  def close(): Unit
}