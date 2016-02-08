package io.eels.component.kafka

import io.eels.{FrameSchema, Part, Source}

class KafkaSource(url: String) extends Source {
  override def parts: Seq[Part] = ???
  override def schema: FrameSchema = ???
}
