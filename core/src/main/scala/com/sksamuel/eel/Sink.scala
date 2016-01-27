package com.sksamuel.eel

trait Sink {
  /**
    * Returns a new, one time use, writer for this sink.
    * This method can be called multiple times, and writers should not share state.
    */
  def writer: Writer
}

trait Writer {

  def write(row: Row)

  def close(): Unit
}