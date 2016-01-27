package com.sksamuel.eel.sink

import com.sksamuel.eel.{Row, Writer, Sink}

class KafkaSink extends Sink {
  override def writer: Writer = new Writer {
    override def close(): Unit = ???
    override def write(row: Row): Unit = ???
  }
}
