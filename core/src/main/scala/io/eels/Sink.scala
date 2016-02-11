package io.eels

trait Sink {
  def writer: Writer
}

trait Writer {
  def write(row: Row): Unit
  def close(): Unit
}