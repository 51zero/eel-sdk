package io.eels

trait Sink {
  def writer: Writer
}

trait Writer {
  def write(row: Row, schema: FrameSchema): Unit
  def close(): Unit
}