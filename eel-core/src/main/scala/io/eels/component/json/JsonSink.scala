package io.eels.component.json

import io.eels.{Row, Sink, SinkWriter}
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.jackson.map.ObjectMapper

case class JsonSink(path: Path)(implicit fs: FileSystem, conf: Configuration) extends Sink {

  override def writer(schema: Schema): SinkWriter = new SinkWriter {

    val lock = new AnyRef()
    val out = fs.create(path)

    val mapper = new ObjectMapper()

    override def write(row: Row) {
      val map = schema.fieldNames.zip(row.values).toMap
      val json = mapper.writeValueAsString(map)
      lock.synchronized {
        out.writeBytes(json)
        out.writeBytes("\n")
      }
    }

    override def close() {
      out.close()
    }
  }
}
