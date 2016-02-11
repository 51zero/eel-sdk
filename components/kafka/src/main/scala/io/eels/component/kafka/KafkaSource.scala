package io.eels.component.kafka

import io.eels.{Reader, FrameSchema, Source}

class KafkaSource(url: String) extends Source {
  override def schema: FrameSchema = ???
  override def readers: Seq[Reader] = ???
}
