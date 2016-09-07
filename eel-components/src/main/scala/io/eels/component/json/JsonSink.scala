package io.eels.component.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.eels.schema.Schema
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.fs.{FileSystem, Path}

case class JsonSink(path: Path)(implicit fs: FileSystem) extends Sink {

  override def writer(schema: Schema): SinkWriter = new SinkWriter {

    val lock = new AnyRef()
    val out = fs.create(path)

    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

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
