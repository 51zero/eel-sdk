package io.eels.component.kafka

import io.eels.{Reader, Row, Source}

class KafkaSource(url: String) extends Source {

  override def reader: Reader = new Reader {
    override def close(): Unit = ???
    override def iterator: Iterator[Row] = ???
  }
}
