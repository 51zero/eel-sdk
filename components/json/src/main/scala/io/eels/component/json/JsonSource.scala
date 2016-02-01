package io.eels.component.json

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.eels.{Column, Field, Reader, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

case class JsonSource(path: Path) extends Source {
  override def reader: Reader = new Reader {

    val fs = FileSystem.get(new Configuration)
    val reader = new ObjectMapper().readerFor(classOf[JsonNode])

    val in = fs.open(path)
    val roots = reader.readValues[JsonNode](in)

    override def close(): Unit = in.close()

    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iter = roots.asScala
      override def hasNext: Boolean = iter.hasNext
      override def next(): Row = nodeToRow(iter.next)
    }

    def nodeToRow(node: JsonNode): Row = {
      val columns = node.fieldNames.asScala.map(Column.apply).toList
      val fields = node.elements.asScala.map(node => Field(node.textValue)).toList
        Row(columns, fields)
    }
  }
}
