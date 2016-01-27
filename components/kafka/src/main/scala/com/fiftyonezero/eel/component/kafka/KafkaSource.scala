package com.fiftyonezero.eel.component.kafka

import com.fiftyonezero.eel.{Reader, Row, Source}

class KafkaSource(url: String) extends Source {

  override def reader: Reader = new Reader {
    override def close(): Unit = ???
    override def iterator: Iterator[Row] = ???
  }
}
