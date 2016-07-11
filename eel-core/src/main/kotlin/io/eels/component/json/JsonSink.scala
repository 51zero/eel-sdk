package io.eels.component.json

import io.eels.schema.Schema
import io.eels.{Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class JsonSink(path: Path) extends Sink {
  self =>

  override def writer(schema: Schema): SinkWriter = new SinkWriter {

    val fs = FileSystem.get(new Configuration)
    val mapper = new ObjectMapper

    val out = fs.create(path)

    override def close(): Unit = out.close()

    override def write(row: InternalRow): Unit = {
      val map = schema.fieldNames.zip(row).toMap.asJava
      val json = mapper.writeValueAsString(map)
      self.synchronized {
        out.writeBytes(json)
        out.writeBytes("\n")
      }
    }
  }
}
