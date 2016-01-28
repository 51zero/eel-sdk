package io.eels.component.kafka

import io.eels.{Row, Sink, Writer}

class KafkaSink extends Sink {
  override def writer: Writer = new Writer {
    override def close(): Unit = ???
    override def write(row: Row): Unit = ???
  }
}
