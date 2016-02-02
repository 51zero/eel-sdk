package io.eels.component.json

import com.fasterxml.jackson.databind.ObjectMapper
import io.eels.{Row, Sink, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.JavaConverters._

case class JsonSink(path: Path) extends Sink {
  override def writer: Writer = new Writer {

    val fs = FileSystem.get(new Configuration)
    val mapper = new ObjectMapper

    val out = fs.create(path)

    override def close(): Unit = out.close()

    override def write(row: Row): Unit = {
      out.writeBytes(mapper.writeValueAsString(row.toMap.asJava))
      out.writeBytes("\n")
    }
  }
}
