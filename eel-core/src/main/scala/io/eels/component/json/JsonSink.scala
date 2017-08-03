package io.eels.component.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.eels.schema.StructType
import io.eels.{Rec, Sink, SinkWriter}
import org.apache.hadoop.fs.{FileSystem, Path}

case class JsonSink(path: Path)(implicit fs: FileSystem) extends Sink {

  override def open(schema: StructType): SinkWriter = new SinkWriter {

    private val lock = new AnyRef()
    private val out = fs.create(path)

    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    override def write(row: Rec) {
      val map = schema.fieldNames.zip(row).toMap
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
