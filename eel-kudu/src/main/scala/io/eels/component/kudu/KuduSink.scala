package io.eels.component.kudu

import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}

class KuduSink extends Sink {
  override def writer(schema: StructType): SinkWriter = new SinkWriter {
    override def write(row: Row): Unit = ???
    override def close(): Unit = ???
  }
}
