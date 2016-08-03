package io.eels.component.json

import io.eels.Row
import io.eels.schema.Schema
import io.eels.Sink
import io.eels.SinkWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.codehaus.jackson.map.ObjectMapper

data class JsonSink(val path: Path) : Sink {

  override fun writer(schema: Schema): SinkWriter = object : SinkWriter {

    val lock = Any()
    val fs = FileSystem.get(Configuration())
    val out = fs.create(path)

    val mapper = ObjectMapper()

    override fun write(row: Row) {
      val map = schema.fieldNames().zip(row.values).toMap()
      val json = mapper.writeValueAsString(map)
      synchronized(lock) {
        out.writeBytes(json)
        out.writeBytes("\n")
      }
    }

    override fun close() {
      out.close()
    }
  }
}
