package io.eels

import io.eels.schema.StructType

/**
  * A sink which just swallows any incoming data.
  * Useful to test throughput speeds of sources.
  */
object DevNullSink extends Sink {
  override def open(schema: StructType): SinkWriter = new SinkWriter {
    override def close(): Unit = ()
    override def write(row: Rec): Unit = ()
  }
}
