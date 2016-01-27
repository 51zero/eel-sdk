package com.sksamuel.eel.source.kafka

import com.sksamuel.eel.{Row, Reader, Source}

class KafkaSource(url: String) extends Source {

  override def reader: Reader = new Reader {
    override def close(): Unit = ???
    override def iterator: Iterator[Row] = ???
  }
}
